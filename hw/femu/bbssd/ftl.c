#include "ftl.h"
#include "dftl.h"

static void *ftl_thread(void *arg);

#ifdef DFTL
static inline bool mapped_ppa(struct ppa *ppa);
static struct ppa get_new_trans_page(struct ssd *ssd);
static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa);
static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct nand_cmd *ncmd);
static void ssd_advance_trans_write_pointer(struct ssd *ssd);
static void mark_trans_page_invalid(struct ssd *ssd, struct ppa *ppa);
static void mark_trans_page_valid(struct ssd *ssd, struct ppa *ppa);
#endif

#ifdef GC
static inline bool should_gc(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines_high);
}
#endif

#ifdef DFTL
static struct cmt_page *cmt_create_page(
    uint64_t gtd_idx,
    struct ppa *ref_data,
    int ppas_per_page)
{
    struct cmt_page *cmt_page = g_malloc0(sizeof(struct cmt_page));
    cmt_page->gtd_idx = gtd_idx;
    cmt_page->is_updated = false;
    cmt_page->data = g_malloc0(sizeof(struct ppa) * ppas_per_page);
    for (int i = 0; i < ppas_per_page; i++) {
        cmt_page->data[i].ppa = ref_data ? ref_data[i].ppa : UNMAPPED_PPA;
    }
    cmt_page->hash_prev = NULL;
    cmt_page->hash_next = NULL;
    cmt_page->less_recently_used = NULL;
    cmt_page->more_recently_used = NULL;

    return cmt_page;
}

static void cmt_insert(
    struct cached_mapping_table *cmt,
    struct cmt_page *page)
{
    ftl_assert(cmt->pgs_cnt <= cmt->max_pgs);
    struct cmt_page *most_recently_used = cmt->most_recently_used;

    if (most_recently_used) most_recently_used->more_recently_used = page;
    else cmt->least_recently_used = page;

    page->less_recently_used = most_recently_used;
    page->more_recently_used = NULL;
    cmt->most_recently_used = page;

    uint64_t hash_index = page->gtd_idx % cmt->hash_tbl_size;
    struct cmt_page *hash_head = cmt->hash_table[hash_index];
    if (hash_head) hash_head->hash_prev = page;
    page->hash_prev = NULL;
    page->hash_next = hash_head;
    cmt->hash_table[hash_index] = page;

    cmt->pgs_cnt++;
}

static struct cmt_page *cmt_pop_victim(struct cached_mapping_table *cmt)
{
    ftl_assert(cmt->pgs_cnt == cmt->max_pgs);
    ftl_assert(cmt->least_recently_used);

    struct cmt_page *victim = cmt->least_recently_used;

    if (victim->more_recently_used) victim->more_recently_used->less_recently_used = NULL;
    else cmt->most_recently_used = NULL;
    cmt->least_recently_used = victim->more_recently_used;

    uint64_t hash_index = victim->gtd_idx % cmt->hash_tbl_size;
    if (victim->hash_prev) victim->hash_prev->hash_next = victim->hash_next;
    else cmt->hash_table[hash_index] = victim->hash_next;

    if (victim->hash_next) victim->hash_next->hash_prev = victim->hash_prev;

    cmt->pgs_cnt--;

    return victim;
}

static bool cmt_is_full(struct cached_mapping_table *cmt)
{
    return cmt->pgs_cnt == cmt->max_pgs;
}

static struct cmt_page *cmt_find_by_gtd(
    struct cached_mapping_table *cmt,
    uint64_t gtd_idx)
{
    uint64_t hash_index = gtd_idx % cmt->hash_tbl_size;

    struct cmt_page *target;
    for (target = cmt->hash_table[hash_index];
         target != NULL && target->gtd_idx != gtd_idx;
         target = target->hash_next);

    if (!target) return NULL;

    if (target->less_recently_used) {
        target->less_recently_used->more_recently_used = target->more_recently_used;
    } else {
        cmt->least_recently_used = target->more_recently_used;
    }

    if (target->more_recently_used) {
        target->more_recently_used->less_recently_used = target->less_recently_used;
    } else {
        cmt->most_recently_used = target->less_recently_used;
    }

    if (target->hash_prev) target->hash_prev->hash_next = target->hash_next;
    else cmt->hash_table[hash_index] = target->hash_next;

    if (target->hash_next) target->hash_next->hash_prev = target->hash_prev;

    cmt->pgs_cnt--;

    cmt_insert(cmt, target);

    return target;
}

static void cmt_destroy_page(struct cmt_page *page)
{
    g_free(page->data);
    g_free(page);
}
#endif

#ifdef DFTL
static struct addr_trans_ops *atops_create(void)
{
    struct addr_trans_ops *atops = g_malloc0(sizeof(struct addr_trans_ops));
    atops->head = NULL;
    atops->tail = NULL;
    return atops;
}

static void atops_enqueue(
    struct addr_trans_ops *atops,
    int nand_cmd,
    struct ppa *ppa)
{
    struct addr_trans_op *op = g_malloc0(sizeof(struct addr_trans_op));
    op->nand_cmd = nand_cmd;
    op->ppa = ppa;
    op->next = NULL;

    struct addr_trans_op *tail = atops->tail;
    if (tail) tail->next = op;
    else atops->head = op;
    atops->tail = op;
}

static void atops_destroy(struct addr_trans_ops *atops)
{
    struct addr_trans_op *next_op = NULL;
    for (struct addr_trans_op *op = atops->head;
         op != NULL;
         op = next_op) {
        next_op = op->next;
        free(op);
    }
    free(atops);
}

static uint64_t get_addr_trans_latency(
    struct ssd *ssd,
    NvmeRequest *req,
    struct addr_trans_ops *atops)
{
    uint64_t latency = 0;

    for (struct addr_trans_op *op = atops->head;
         op != NULL;
         op = op->next) {

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = op->nand_cmd;
        srd.stime = req->stime;
        latency += ssd_advance_status(ssd, op->ppa, &srd);
    }

    return latency;
}
#endif

#ifdef DFTL
static void evict_cmt_if_full(
    struct ssd *ssd,
    struct cached_mapping_table *cmt,
    struct addr_trans_ops *atops)
{
    struct ssdparams *spp = &ssd->sp;

    if (!cmt_is_full(cmt)) return;

    struct cmt_page *victim = cmt_pop_victim(cmt);
    if (victim->is_updated) {
        struct ppa new_trans_ppa = get_new_trans_page(ssd);
        struct nand_page *new_trans_page = get_pg(ssd, &new_trans_ppa);

        for (int i = 0; i < spp->ppas_per_page; i++) {
            new_trans_page->trans_data[i] = victim->data[i];
        }

        mark_trans_page_invalid(ssd, &ssd->gtd[victim->gtd_idx]);
        ssd->gtd[victim->gtd_idx] = new_trans_ppa;
        mark_trans_page_valid(ssd, &new_trans_ppa);
        ssd_advance_trans_write_pointer(ssd);

        atops_enqueue(atops, NAND_WRITE, &new_trans_ppa);
    }
    cmt_destroy_page(victim);
}
#endif

#ifndef DFTL
static inline struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn)
{
    return ssd->maptbl[lpn];
}
#else
static struct ppa get_maptbl_ent(
    struct ssd *ssd,
    uint64_t lpn,
    struct addr_trans_ops *atops)
{
    struct ssdparams *spp = &ssd->sp;
    struct cached_mapping_table *cmt = &ssd->cmt;

    uint64_t gtd_idx = lpn / spp->ppas_per_page;
    uint64_t offset = lpn % spp->ppas_per_page;

    struct cmt_page *cached_trans_page = cmt_find_by_gtd(cmt, gtd_idx);

    if (!cached_trans_page) {
        struct ppa *trans_pg_ppa = &ssd->gtd[gtd_idx];

        evict_cmt_if_full(ssd, cmt, atops);

        struct ppa *ref_data = NULL;
        if (mapped_ppa(trans_pg_ppa)) {
            struct nand_page *npg = get_pg(ssd, trans_pg_ppa);
            ref_data = npg->trans_data;
            atops_enqueue(atops, NAND_READ, trans_pg_ppa);
        }

        cached_trans_page = cmt_create_page(gtd_idx, ref_data, spp->ppas_per_page);

        cmt_insert(cmt, cached_trans_page);
    }

    return cached_trans_page->data[offset];
}
#endif

#ifndef DFTL
static inline void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < ssd->sp.tt_pgs);
    ssd->maptbl[lpn] = *ppa;
}
#else
static void set_maptbl_ent(
    struct ssd *ssd,
    uint64_t lpn,
    struct ppa *ppa,
    struct addr_trans_ops *atops)
{
    struct ssdparams *spp = &ssd->sp;
    struct cached_mapping_table *cmt = &ssd->cmt;

    uint64_t gtd_idx = lpn / spp->ppas_per_page;
    uint64_t offset = lpn % spp->ppas_per_page;

    struct cmt_page *cached_trans_page = cmt_find_by_gtd(cmt, gtd_idx);

    if (!cached_trans_page) {
        struct ppa *orig_trans_ppa = &ssd->gtd[gtd_idx];

        evict_cmt_if_full(ssd, cmt, atops);

        struct ppa *ref_data = NULL;
        if (mapped_ppa(orig_trans_ppa)) {
            struct nand_page *npg = get_pg(ssd, orig_trans_ppa);
            ref_data = npg->trans_data;
            atops_enqueue(atops, NAND_READ, orig_trans_ppa);
        }

        cached_trans_page = cmt_create_page(gtd_idx, ref_data, spp->ppas_per_page);

        cmt_insert(cmt, cached_trans_page);
    }

    cached_trans_page->is_updated = true;
    cached_trans_page->data[offset] = *ppa;
}
#endif

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch  * spp->pgs_per_ch  + \
            ppa->g.lun * spp->pgs_per_lun + \
            ppa->g.pl  * spp->pgs_per_pl  + \
            ppa->g.blk * spp->pgs_per_blk + \
            ppa->g.pg;

    ftl_assert(pgidx < spp->tt_pgs);

    return pgidx;
}

static inline uint64_t get_rmap_ent(struct ssd *ssd, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
    return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
    return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
    ((struct line *)a)->pos = pos;
}

static void ssd_init_lines(
    struct ssdparams *spp,
    struct line_mgmt *lm,
    bool (*blk_filter_fn)(int))
{
    struct line *line;

    lm->tt_lines = 0;
    for (int i = 0; i < spp->tt_lines; i++) {
        if (blk_filter_fn(i)) lm->tt_lines++;
    }

    lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->free_line_list);
    lm->victim_line_pq = pqueue_init(lm->tt_lines, victim_line_cmp_pri,
            victim_line_get_pri, victim_line_set_pri,
            victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->full_line_list);

    int line_idx = 0;
    lm->free_line_cnt = 0;
    for (int i = 0; i < spp->tt_lines; i++) {
        if (!blk_filter_fn(i)) continue;
        line = &lm->lines[line_idx];
        line->id = line_idx++;
        line->blk = i;
        line->ipc = 0;
        line->vpc = 0;
        line->pos = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
        lm->free_line_cnt++;
    }

    ftl_assert(line_idx == lm->tt_lines);
    ftl_assert(lm->free_line_cnt == lm->tt_lines);
    lm->victim_line_cnt = 0;
    lm->full_line_cnt = 0;
}
#ifdef DFTL
static bool is_trans_block_num(int block_num)
{
    return block_num % 8 == 0;
}

static void ssd_init_trans_lines(struct ssd *ssd)
{
    ssd_init_lines(&ssd->sp, &ssd->trans_lm, is_trans_block_num);
}
#endif

static bool is_data_block_num(int block_num)
{
#ifdef DFTL
    return block_num % 8 != 0;
#else
    return true;
#endif
}

static void ssd_init_data_lines(struct ssd *ssd)
{
    ssd_init_lines(&ssd->sp, &ssd->data_lm, is_data_block_num);
}

static void ssd_init_write_pointer(
    struct write_pointer *wpp,
    struct line_mgmt *lm)
{
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    /* wpp->curline is always our next-to-write super-block */
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = curline->blk;
    wpp->pl = 0;
}

#ifdef DFTL
static void ssd_init_trans_write_pointer(struct ssd *ssd)
{
    ssd_init_write_pointer(&ssd->trans_wp, &ssd->trans_lm);
}
#endif

static void ssd_init_data_write_pointer(struct ssd *ssd)
{
    ssd_init_write_pointer(&ssd->data_wp, &ssd->data_lm);
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static struct line *get_next_free_line(
    char *ssd_name,
    struct line_mgmt *lm)
{
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        ftl_err("No free lines left in [%s] !!!!\n", ssd_name);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    return curline;
}

static void ssd_advance_write_pointer(
    struct ssd *ssd,
    struct write_pointer *wpp,
    struct line_mgmt *lm)
{
    struct ssdparams *spp = &ssd->sp;

    check_addr(wpp->ch, spp->nchs);
    wpp->ch++;
    if (wpp->ch == spp->nchs) {
        wpp->ch = 0;
        check_addr(wpp->lun, spp->luns_per_ch);
        wpp->lun++;
        /* in this case, we should go to next lun */
        if (wpp->lun == spp->luns_per_ch) {
            wpp->lun = 0;
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk);
            wpp->pg++;
            if (wpp->pg == spp->pgs_per_blk) {
                wpp->pg = 0;
                /* move current line to {victim,full} line list */
                if (wpp->curline->vpc == spp->pgs_per_line) {
                    /* all pgs are still valid, move to full line list */
                    ftl_assert(wpp->curline->ipc == 0);
                    QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry);
                    lm->full_line_cnt++;
                } else {
                    ftl_assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
                    /* there must be some invalid pages in this line */
                    ftl_assert(wpp->curline->ipc > 0);
                    pqueue_insert(lm->victim_line_pq, wpp->curline);
                    lm->victim_line_cnt++;
                }
                /* current line is used up, pick another empty line */
                check_addr(wpp->blk, spp->blks_per_pl);
                wpp->curline = NULL;
                wpp->curline = get_next_free_line(ssd->ssdname, lm);
                if (!wpp->curline) {
                    /* TODO */
                    abort();
                }
                wpp->blk = wpp->curline->blk;
                check_addr(wpp->blk, spp->blks_per_pl);
                /* make sure we are starting from page 0 in the super block */
                ftl_assert(wpp->pg == 0);
                ftl_assert(wpp->lun == 0);
                ftl_assert(wpp->ch == 0);
                /* TODO: assume # of pl_per_lun is 1, fix later */
                ftl_assert(wpp->pl == 0);
            }
        }
    }
}

#ifdef DFTL
static void ssd_advance_trans_write_pointer(struct ssd *ssd)
{
    ssd_advance_write_pointer(ssd, &ssd->trans_wp, &ssd->trans_lm);
}
#endif

static void ssd_advance_data_write_pointer(struct ssd *ssd)
{
    ssd_advance_write_pointer(ssd, &ssd->data_wp, &ssd->data_lm);
}


static struct ppa get_new_page(struct write_pointer *wpp)
{
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    ftl_assert(ppa.g.pl == 0);

    return ppa;
}

#ifdef DFTL
static struct ppa get_new_trans_page(struct ssd *ssd)
{
    return get_new_page(&ssd->trans_wp);
}
#endif

static struct ppa get_new_data_page(struct ssd *ssd)
{
    return get_new_page(&ssd->data_wp);
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    //ftl_assert(is_power_of_2(spp->luns_per_ch));
    //ftl_assert(is_power_of_2(spp->nchs));
}

static void ssd_init_params(struct ssdparams *spp)
{
    spp->secsz = 512;
    spp->secs_per_pg = 8;
    spp->pgs_per_blk = 256;
    spp->blks_per_pl = 256; /* 16GB */
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 8;
    spp->nchs = 8;

    spp->pg_rd_lat = NAND_READ_LATENCY;
    spp->pg_wr_lat = NAND_PROG_LATENCY;
    spp->blk_er_lat = NAND_ERASE_LATENCY;
    spp->ch_xfer_lat = 0;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    spp->gc_thres_pcent = 0.75;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->gc_thres_pcent_high = 0.95;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);
    spp->enable_gc_delay = true;

    #ifdef DFTL
    spp->ppas_per_page = spp->secs_per_pg * spp->secsz / sizeof(struct ppa);
    #endif

    check_params(spp);
}

static void ssd_init_nand_page(
    struct nand_page *pg,
    #ifdef DFTL
    bool is_trans,
    #endif
    struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;

    #ifdef DFTL
    if (is_trans) {
        pg->trans_data = g_malloc0(sizeof(struct ppa) * spp->ppas_per_page);
        for (int i = 0; i < spp->ppas_per_page; i++) {
            pg->trans_data[i].ppa = UNMAPPED_PPA;
        }
    } else {
        pg->trans_data = NULL;
    }
    #endif
}

static void ssd_init_nand_blk(
    struct nand_block *blk,
    #ifdef DFTL
    bool is_trans,
    #endif
    struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(
            &blk->pg[i],
            #ifdef DFTL
            is_trans,
            #endif
            spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(
            &pl->blk[i],
            #ifdef DFTL
            is_trans_block_num(i),
            #endif
            spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

#ifdef DFTL
static void ssd_init_gtd(struct ssd *ssd) {
    struct ssdparams *spp = &ssd->sp;

    int gtd_size = spp->tt_pgs / spp->ppas_per_page;
    ssd->gtd = g_malloc0(sizeof(struct ppa) * gtd_size);
    for (int i = 0; i < gtd_size; i++) {
        ssd->gtd[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_cmt(struct ssd *ssd) {
    struct ssdparams *spp = &ssd->sp;
    struct cached_mapping_table *cmt = &ssd->cmt;

    cmt->pgs_cnt = 0;
    cmt->max_pgs = CMT_SIZE / (spp->secs_per_pg * spp->secsz);
    cmt->hash_tbl_size = cmt->max_pgs / CMT_HASH_DIV_FACTOR;
    cmt->least_recently_used = NULL;
    cmt->most_recently_used = NULL;
    cmt->hash_table = g_malloc0(sizeof(struct cmt_page *) * cmt->hash_tbl_size);
    for (int i = 0; i < cmt->hash_tbl_size; i++) {
        cmt->hash_table[i] = NULL;
    }
}
#else
static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}
#endif

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i] = INVALID_LPN;
    }
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(spp);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    #ifdef DFTL
    ssd_init_gtd(ssd);
    ssd_init_cmt(ssd);
    #else
    ssd_init_maptbl(ssd);
    #endif

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    #ifdef DFTL
    ssd_init_trans_lines(ssd);
    #endif
    ssd_init_data_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
    #ifdef DFTL
    ssd_init_trans_write_pointer(ssd);
    #endif
    ssd_init_data_write_pointer(ssd);

    qemu_thread_create(&ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n,
                       QEMU_THREAD_JOINABLE);
}

static inline bool valid_ppa(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch && pl >=
        0 && pl < spp->pls_per_lun && blk >= 0 && blk < spp->blks_per_pl && pg
        >= 0 && pg < spp->pgs_per_blk && sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct ssd *ssd, uint64_t lpn)
{
    return (lpn < ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct line *get_line(struct line_mgmt *lm, struct ppa *ppa)
{
    return &(lm->lines[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
#if 0
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (ncmd->type == USER_IO) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        }
        lat = lun->next_lun_avail_time - cmd_stime;

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat;
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(
    struct ssd *ssd,
    struct line_mgmt *lm,
    struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    /* update corresponding page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    ftl_assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(lm, ppa);
    ftl_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line) {
        ftl_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    ftl_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos) {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
    } else {
        line->vpc--;
    }

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}

#ifdef DFTL
static void mark_trans_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    return mark_page_invalid(ssd, &ssd->trans_lm, ppa);
}
#endif

static void mark_data_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    return mark_page_invalid(ssd, &ssd->data_lm, ppa);
}

static void mark_page_valid(
    struct ssd *ssd,
    struct line_mgmt *lm,
    struct ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(lm, ppa);
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line);
    line->vpc++;
}

#ifdef DFTL
static void mark_trans_page_valid(struct ssd *ssd, struct ppa *ppa)
{
    return mark_page_valid(ssd, &ssd->trans_lm, ppa);
}
#endif

static void mark_data_page_valid(struct ssd *ssd, struct ppa *ppa)
{
    return mark_page_valid(ssd, &ssd->data_lm, ppa);
}

#ifdef GC
static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    ftl_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

static void gc_read_page(struct ssd *ssd, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ssd, ppa, &gcr);
    }
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct ssd *ssd, struct ppa *old_ppa)
{
    struct ppa new_ppa;
    struct nand_lun *new_lun;
    uint64_t lpn = get_rmap_ent(ssd, old_ppa);

    ftl_assert(valid_lpn(ssd, lpn));
    new_ppa = get_new_data_page(ssd);
    /* update maptbl */
    set_maptbl_ent(ssd, lpn, &new_ppa);
    /* update rmap */
    set_rmap_ent(ssd, lpn, &new_ppa);

    mark_page_valid(ssd, &new_ppa);

    /* need to advance the write pointer here */
    ssd_advance_write_data_pointer(ssd);

    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw);
    }

    /* advance per-ch gc_endtime as well */
#if 0
    new_ch = get_ch(ssd, &new_ppa);
    new_ch->gc_endtime = new_ch->next_ch_avail_time;
#endif

    new_lun = get_lun(ssd, &new_ppa);
    new_lun->gc_endtime = new_lun->next_lun_avail_time;

    return 0;
}

static struct line *select_victim_line(struct ssd *ssd, bool force)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *victim_line = NULL;

    victim_line = pqueue_peek(lm->victim_line_pq);
    if (!victim_line) {
        return NULL;
    }

    if (!force && victim_line->ipc < ssd->sp.pgs_per_line / 8) {
        return NULL;
    }

    pqueue_pop(lm->victim_line_pq);
    victim_line->pos = 0;
    lm->victim_line_cnt--;

    /* victim_line is a danggling node now */
    return victim_line;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        ftl_assert(pg_iter->status != PG_FREE);
        if (pg_iter->status == PG_VALID) {
            gc_read_page(ssd, ppa);
            /* delay the maptbl update until "write" happens */
            gc_write_page(ssd, ppa);
            cnt++;
        }
    }

    ftl_assert(get_blk(ssd, ppa)->vpc == cnt);
}

static void mark_line_free(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *line = get_line(ssd, ppa);
    line->ipc = 0;
    line->vpc = 0;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
}

static int do_gc(struct ssd *ssd, bool force)
{
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lunp;
    struct ppa ppa;
    int ch, lun;

    victim_line = select_victim_line(ssd, force);
    if (!victim_line) {
        return -1;
    }

    ppa.g.blk = victim_line->id;
    ftl_debug("GC-ing line:%d,ipc=%d,victim=%d,full=%d,free=%d\n", ppa.g.blk,
              victim_line->ipc, ssd->lm.victim_line_cnt, ssd->lm.full_line_cnt,
              ssd->lm.free_line_cnt);

    /* copy back valid data */
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            lunp = get_lun(ssd, &ppa);
            clean_one_block(ssd, &ppa);
            mark_block_free(ssd, &ppa);

            if (spp->enable_gc_delay) {
                struct nand_cmd gce;
                gce.type = GC_IO;
                gce.cmd = NAND_ERASE;
                gce.stime = 0;
                ssd_advance_status(ssd, &ppa, &gce);
            }

            lunp->gc_endtime = lunp->next_lun_avail_time;
        }
    }

    /* update line status */
    mark_line_free(ssd, &ppa);

    return 0;
}
#endif

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        sublat = 0;

        #ifndef DFTL
        ppa = get_maptbl_ent(ssd, lpn);
        #else
        struct addr_trans_ops *atops = atops_create();

        ppa = get_maptbl_ent(ssd, lpn, atops);

        sublat += get_addr_trans_latency(ssd, req, atops);
        atops_destroy(atops);
        #endif

        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
            //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat += ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    #ifdef GC
    int r;
    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true);
        if (r == -1)
            break;
    }
    #endif

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        curlat = 0;

        #ifndef DFTL
        ppa = get_maptbl_ent(ssd, lpn);
        #else
        struct addr_trans_ops *atops = atops_create();

        ppa = get_maptbl_ent(ssd, lpn, atops);
        #endif

        if (mapped_ppa(&ppa)) {
            /* update old page information first */
            mark_data_page_invalid(ssd, &ppa);
            set_rmap_ent(ssd, INVALID_LPN, &ppa);
        }

        /* new write */
        ppa = get_new_data_page(ssd);
        /* update maptbl */
        #ifndef DFTL
        set_maptbl_ent(ssd, lpn, &ppa);
        #else
        set_maptbl_ent(ssd, lpn, &ppa, atops);

        curlat += get_addr_trans_latency(ssd, req, atops);
        atops_destroy(atops);
        #endif
        /* update rmap */
        set_rmap_ent(ssd, lpn, &ppa);

        mark_data_page_valid(ssd, &ppa);

        /* need to advance the write pointer here */
        ssd_advance_data_write_pointer(ssd);

        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat += ssd_advance_status(ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct ssd *ssd = n->ssd;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;

    while (1) {
        for (i = 1; i <= n->num_poller; i++) {
            if (!ssd->to_ftl[i] || !femu_ring_count(ssd->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(ssd->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);
            switch (req->cmd.opcode) {
            case NVME_CMD_WRITE:
                lat = ssd_write(ssd, req);
                break;
            case NVME_CMD_READ:
                lat = ssd_read(ssd, req);
                break;
            case NVME_CMD_DSM:
                lat = 0;
                break;
            default:
                //ftl_err("FTL received unkown request type, ERROR\n");
                ;
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            #ifdef GC
            /* clean one line if needed (in the background) */
            if (should_gc(ssd)) {
                do_gc(ssd, false);
            }
            #endif
        }
    }

    return NULL;
}
