#include "ftl.h"
#include "avltree.h"

//#define FEMU_DEBUG_FTL

static void *ftl_thread(void *arg);

static inline bool should_gc(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines_high);
}

static inline struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn)
{
    return ssd->maptbl[lpn];
}

static inline void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < ssd->sp.tt_pgs);
    ssd->maptbl[lpn] = *ppa;
}

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

static void ssd_init_lines(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *line;

    lm->tt_lines = spp->blks_per_pl;
    ftl_assert(lm->tt_lines == spp->tt_lines);
    lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->free_line_list);
    lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri,
            victim_line_get_pri, victim_line_set_pri,
            victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->full_line_list);

    lm->free_line_cnt = 0;
    for (int i = 0; i < lm->tt_lines; i++) {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        line->pos = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
        lm->free_line_cnt++;
    }

    ftl_assert(lm->free_line_cnt == lm->tt_lines);
    lm->victim_line_cnt = 0;
    lm->full_line_cnt = 0;
}

static void ssd_init_write_pointer(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    /* wpp->curline is always our next-to-write super-block */
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = 0;
    wpp->pl = 0;
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct ssd *ssd)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        ftl_err("No free lines left in [%s] !!!!\n", ssd->ssdname);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    return curline;
}

static void ssd_advance_write_pointer(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;

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
                wpp->curline = get_next_free_line(ssd);
                if (!wpp->curline) {
                    /* TODO */
                    abort();
                }
                wpp->blk = wpp->curline->id;
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

static struct ppa get_new_page(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp;
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
    spp->secsz = SEC_SIZE;
    spp->secs_per_pg = SECS_PER_PG;
    spp->pgs_per_blk = 256;
    // spp->blks_per_pl = 256; /* 16GB */
    spp->blks_per_pl = 80; /* 5 GB */
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


    check_params(spp);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
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
        ssd_init_nand_blk(&pl->blk[i], spp);
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

static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i] = INVALID_LPN;
    }
}

static void ssd_init_buffer(struct ssd *ssd, uint32_t dramsz_mb) {
    uint32_t secs_cnt = dramsz_mb * 1024 * 1024 / SEC_SIZE;
    ssd->wbuffer=avlTreeCreate((void*)keyCompareFunc, (void *)freeFunc);
    switch (BUFFER_SCHEME)
    {
    /* ssd->wbuffer and ssd->rbuffer is in fact the same buffer. */
    case READ_WRITE_HYBRID:
        ssd->wbuffer->max_secs = secs_cnt;
        ssd->rbuffer=ssd->wbuffer;
        break;
    
    /*  */
    case READ_WRITE_PARTITION:
        ssd->rbuffer=avlTreeCreate((void*)keyCompareFunc, (void *)freeFunc);
        ssd->wbuffer->max_secs = secs_cnt / 2;
        ssd->rbuffer->max_secs = secs_cnt - ssd->wbuffer->max_secs;
        break;

    default:
        ftl_assert(0);
        break;
    }
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(spp);

    ssd_init_buffer(ssd, n->dramsz);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    ssd_init_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
    ssd_init_write_pointer(ssd);

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

static inline struct line *get_line(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->lm.lines[ppa->g.blk]);
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
static void mark_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
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
    line = get_line(ssd, ppa);
    ftl_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line) {
        ftl_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    ftl_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    if (line->pos) {
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

static void mark_page_valid(struct ssd *ssd, struct ppa *ppa)
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
    line = get_line(ssd, ppa);
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line);
    line->vpc++;
}

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
    new_ppa = get_new_page(ssd);
    /* update maptbl */
    set_maptbl_ent(ssd, lpn, &new_ppa);
    /* update rmap */
    set_rmap_ent(ssd, lpn, &new_ppa);

    mark_page_valid(ssd, &new_ppa);

    /* need to advance the write pointer here */
    ssd_advance_write_pointer(ssd);

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

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    if (nsecs == 0) return 0;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("[%s]start_lpn=%"PRIu64",end_lpn=%"PRIu64",tt_pgs=%d\n",__FUNCTION__, start_lpn, end_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        uint32_t state = 0, offset1 = 0, offset2 = spp->secs_per_pg - 1;
        if (lpn == start_lpn)
            offset1 = lba - lpn * spp->secs_per_pg;
        if (lpn == end_lpn)
            offset2 = (lba + nsecs - 1) % spp->secs_per_pg;

        // set_valid
        for (int i=offset1; i<=offset2; ++i) {
            state |= (1<<i);
        }

        // printf("[%s]: lpn=%lu, state=%u\n", __FUNCTION__, lpn, state);

        sublat = buffer_read(ssd, req, lpn, state);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

/*
 * The codes has two problem:
 * 1. the interaction of read/write buffer is not considered.
 * 2. multiple pages are written to buffer serially, but only one buffer write delay is returned.
 */

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;

    /* state is stored in a uint32_t type. */
    ftl_assert(spp->secs_per_pg <= 32);

    int len = req->nlb;

    /* Border Situation. */
    if (len == 0) return 0;

    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",end_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, end_lpn, ssd->sp.tt_pgs);
    }

    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true);
        if (r == -1)
            break;
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++){
        uint32_t state = 0, offset1 = 0, offset2 = spp->secs_per_pg - 1;
        if (lpn == start_lpn)
            offset1 = lba - lpn * spp->secs_per_pg;
        if (lpn == end_lpn)
            offset2 = (lba + len - 1) % spp->secs_per_pg;

        // set_valid
        for (int i=offset1; i<=offset2; ++i) {
            state |= (1<<i);
        }

        // printf("[%s]: lpn=%lu, state=%u\n", __FUNCTION__, lpn, state);

        curlat = buffer_write(ssd, req, lpn, state);
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
            switch (req->is_write) {
            case 1:
                lat = ssd_write(ssd, req);
                // printf("Write lats = %lu\n", lat);
                break;
            case 0:
                lat = ssd_read(ssd, req);
                // printf("Read lats = %lu\n", lat);
                break;
            default:
                ftl_err("FTL received unkown request type, ERROR\n");
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            /* clean one line if needed (in the background) */
            if (should_gc(ssd)) {
                do_gc(ssd, false);
            }
        }
    }

    return NULL;
}


inline uint32_t bit_count(uint32_t state) {
    uint32_t res = 0;
    while (state) {
        state &= (state-1);
        res ++;
    }
    return res;
}

int keyCompareFunc(TREE_NODE *p1, TREE_NODE *p2)
{
	struct buffer_group *T1=NULL,*T2=NULL;

	T1=(struct buffer_group*)p1;
	T2=(struct buffer_group*)p2;

	if(T1->group< T2->group) return 1;
	if(T1->group> T2->group) return -1;

	return 0;
}

int freeFunc(TREE_NODE *pNode)
{
	
	if(pNode!=NULL)
	{
		free((void *)pNode);
	}
	
	pNode=NULL;
	return 1;
}

/* 
 * Search for buffer node with specific LPN;
 * return NULL (not found) or a ptr.
 */
static inline struct buffer_group* buffer_search(tAVLTree *buffer, uint64_t lpn) {
    struct buffer_group key;
    key.group = lpn;
    return (struct buffer_group*)avlTreeFind(buffer, (TREE_NODE *)&key);
}

/*
 * Evict the node at the tail (i.e., the MRU side) of the LRU.
 * Return  latency.
 */
static uint64_t buffer_evict(struct ssd *ssd, tAVLTree *buffer, NvmeRequest *req) {
    uint64_t lat = 0;

    struct buffer_group *pt = buffer->buffer_tail;

    uint32_t state =  pt->stored;
    uint64_t lpn = pt->group;
    bool is_dirty = pt->is_dirty;

    // Flush to NAND flash.
    if (is_dirty) {
        struct ppa ppa = get_maptbl_ent(ssd, lpn);
        if (mapped_ppa(&ppa)) {
            mark_page_invalid(ssd, &ppa);
            set_rmap_ent(ssd, INVALID_LPN, &ppa);
        }

         /* new write */
        ppa = get_new_page(ssd);
        /* update maptbl */
        set_maptbl_ent(ssd, lpn, &ppa);
        /* update rmap */
        set_rmap_ent(ssd, lpn, &ppa);

        mark_page_valid(ssd, &ppa);

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ssd);
        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        lat += ssd_advance_status(ssd, &ppa, &swr);
    }

    /* Delete node. */
    buffer->secs_cnt -= bit_count(state);
    avlTreeDel(buffer, (TREE_NODE *)pt);
    if (buffer->buffer_head->LRU_link_next == NULL){ /* One node only. */
        buffer->buffer_head = NULL;
        buffer->buffer_tail = NULL;
    }
    else{
        buffer->buffer_tail = buffer->buffer_tail->LRU_link_pre;
        buffer->buffer_tail->LRU_link_next = NULL;
    }
    pt->LRU_link_next = NULL;
    pt->LRU_link_pre = NULL;
    AVL_TREENODE_FREE(buffer, (TREE_NODE *)pt);
    pt = NULL;

    return lat;
}

static void buffer_delete_rnode(struct ssd *ssd, uint64_t lpn) {
    tAVLTree *rbuffer = ssd->rbuffer;
    struct buffer_group *rnode = buffer_search(rbuffer, lpn);
    if (rnode != NULL) {
        uint32_t nsecs = bit_count(rnode->stored);
        rbuffer->secs_cnt -= nsecs;
        
        // delete in LRU list.
        if (rnode == rbuffer->buffer_head) {
            if (rnode == rbuffer->buffer_tail) {
                rbuffer->buffer_tail = NULL;
                rbuffer->buffer_head = NULL;
            }
            else {
                rbuffer->buffer_head = rnode->LRU_link_next;
                rbuffer->buffer_head->LRU_link_pre = NULL;
            }
            
        }
        else if (rnode == rbuffer->buffer_tail) {
            rbuffer->buffer_tail = rbuffer->buffer_tail->LRU_link_pre;
            rbuffer->buffer_tail->LRU_link_next = NULL;
        }
        else {
            rnode->LRU_link_next->LRU_link_pre = rnode->LRU_link_pre;
            rnode->LRU_link_pre->LRU_link_next = rnode->LRU_link_next;
        }
        rnode->LRU_link_next = NULL;
        rnode->LRU_link_pre = NULL;

        //delete in avlTree.
        avlTreeDel(rbuffer, (TREE_NODE *)rnode);
        AVL_TREENODE_FREE(rbuffer, (TREE_NODE *)rnode);
    }
    return;
}

uint64_t buffer_write(struct ssd *ssd, NvmeRequest *req, uint64_t lpn, uint32_t state) {
    uint64_t lat = DRAM_WRITE_LATENCY;
    tAVLTree *buffer = ssd->wbuffer;
    struct buffer_group *old_node = buffer_search(buffer, lpn);
    
    uint32_t nsecs = 0;


    // 读写buffer一致性的简单处理：在写的时候直接将整个node在读buffer中删除
    if (BUFFER_SCHEME == READ_WRITE_PARTITION) {
        buffer_delete_rnode(ssd, lpn);
    }
    

    /* buffer miss */
    if (old_node == NULL) {
        /* create a new */
        struct buffer_group *new_node = NULL;
        new_node = (struct buffer_group *)malloc(sizeof(struct buffer_group));
        ftl_assert(new_node);
        new_node->group = lpn;
        new_node->stored = state;
        new_node->is_dirty = true;

        nsecs = bit_count(state);

        /* Insert to LRU head. */
        new_node->LRU_link_pre = NULL;
        new_node->LRU_link_next = buffer->buffer_head;
        if (buffer->buffer_head != NULL){
            buffer->buffer_head->LRU_link_pre = new_node;
        }
        else{
            buffer->buffer_tail = new_node;
        }
        buffer->buffer_head = new_node;
        new_node->LRU_link_pre = NULL;
        avlTreeAdd(buffer, (TREE_NODE *)new_node);
    }
    else {
        old_node->is_dirty = true;
        /* partial hit */
        if ((state & (~old_node->stored)) != 0) {
            uint32_t new_state = state | old_node->stored;
            nsecs = bit_count(new_state) - bit_count(old_node->stored);
            old_node->stored = new_state;
        }

        /* Move to LRU head. */
        if (buffer->buffer_head != old_node)
        {
            if (buffer->buffer_tail == old_node)
            {
                buffer->buffer_tail = old_node->LRU_link_pre;
                old_node->LRU_link_pre->LRU_link_next = NULL;
            }
            else if (old_node != buffer->buffer_head)
            {
                old_node->LRU_link_pre->LRU_link_next = old_node->LRU_link_next;
                old_node->LRU_link_next->LRU_link_pre = old_node->LRU_link_pre;
            }
            old_node->LRU_link_next = buffer->buffer_head;
            buffer->buffer_head->LRU_link_pre = old_node;
            old_node->LRU_link_pre = NULL;
            buffer->buffer_head = old_node;
        }
    }

    buffer->secs_cnt += nsecs;
    while (buffer->secs_cnt > buffer->max_secs) {
        lat += buffer_evict(ssd, buffer, req);
    }

    return lat;
}

static uint32_t read_from_wbuffer(struct ssd *ssd, uint64_t lpn, uint32_t state) {
    tAVLTree *wbuffer = ssd->wbuffer;
    struct buffer_group *wnode = buffer_search(wbuffer, lpn);
    if (wnode != NULL) {
        state &= (~wnode->stored);
        if (state == 0) {
            wbuffer->read_hit ++;
        }
        else {
            wbuffer->read_partial_hit ++;
        }
    }
    else {
        wbuffer->read_miss ++;
    }
    return state;
}

uint64_t buffer_read(struct ssd *ssd, NvmeRequest *req, uint64_t lpn, uint32_t state) {
    uint64_t lat = DRAM_READ_LATENCY;

    /* check write buffer. */
    if (BUFFER_SCHEME == READ_WRITE_PARTITION) {
        state = read_from_wbuffer(ssd, lpn, state);
        if (state == 0) { //fully hit in write buffer.
            return lat;
        }
    }    

    tAVLTree *buffer = ssd->rbuffer;
    struct buffer_group *old_node = buffer_search(buffer, lpn);

    uint32_t nsecs = 0;

    /* buffer miss */
    if (old_node == NULL) {
        struct buffer_group *new_node = NULL;
        new_node = (struct buffer_group *)malloc(sizeof(struct buffer_group));
        ftl_assert(new_node);
        new_node->group = lpn;
        new_node->stored = state;
        new_node->is_dirty = false;
        
        nsecs = bit_count(state);

        /* Insert to LRU head. */
        new_node->LRU_link_pre = NULL;
        new_node->LRU_link_next = buffer->buffer_head;
        if (buffer->buffer_head != NULL){
            buffer->buffer_head->LRU_link_pre = new_node;
        }
        else{
            buffer->buffer_tail = new_node;
        }
        buffer->buffer_head = new_node;
        new_node->LRU_link_pre = NULL;
        avlTreeAdd(buffer, (TREE_NODE *)new_node);

        /* NAND flash. */
        struct ppa ppa = get_maptbl_ent(ssd, lpn);
        if (mapped_ppa(&ppa) && valid_ppa(ssd, &ppa)) {
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            srd.stime = req->stime;
            lat += ssd_advance_status(ssd, &ppa, &srd);
        }
    }
    else {
        /* Move to LRU head. */
        if (buffer->buffer_head != old_node) {
            /* Isolate from LRU. */
            if (buffer->buffer_tail == old_node) {
                buffer->buffer_tail = old_node->LRU_link_pre;
                old_node->LRU_link_pre->LRU_link_next = NULL;
            }
            else {
                ftl_assert(old_node != buffer->buffer_head);
                old_node->LRU_link_pre->LRU_link_next = old_node->LRU_link_next;
                old_node->LRU_link_next->LRU_link_pre = old_node->LRU_link_pre;
            }
            /* Insert to head of LRU. */
            old_node->LRU_link_next = buffer->buffer_head;
            buffer->buffer_head->LRU_link_pre = old_node;
            old_node->LRU_link_pre = NULL;
            buffer->buffer_head = old_node;
        }

        /* Partial hit */
        // if (old_node->stored != state) {
        if ((state & (~old_node->stored)) != 0) {
            uint32_t new_state = (state | old_node->stored);
            nsecs = bit_count(new_state) - bit_count(old_node->stored);

            old_node->stored = new_state;

            /* NAND flash. */
            struct ppa ppa = get_maptbl_ent(ssd, lpn);
            if (mapped_ppa(&ppa) && valid_ppa(ssd, &ppa)) {
                struct nand_cmd srd;
                srd.type = USER_IO;
                srd.cmd = NAND_READ;
                srd.stime = req->stime;
                lat += ssd_advance_status(ssd, &ppa, &srd);
            }
        }
    }

    buffer->secs_cnt += nsecs;
    while (buffer->secs_cnt > buffer->max_secs) {
        lat += buffer_evict(ssd, buffer, req);
    }

    return lat;
}

static void buffer_print_lru(tAVLTree *buffer) {
#ifdef FEMU_DEBUG_FTL
    uint32_t secs_cnt = 0;
#endif //FEMU_DEBUG_FTL
    struct buffer_group *node = buffer->buffer_head;
    while (node != NULL) {
#ifdef FEMU_DEBUG_FTL
        secs_cnt += bit_count(node->stored);
#endif //FEMU_DEBUG_FTL
        printf("%u[%u]->\n", node->group, node->stored);
        fflush(stdout);
        node = node->LRU_link_next;
    }
#ifdef FEMU_DEBUG_FTL
    ftl_assert(secs_cnt == buffer->secs_cnt);
#endif //FEMU_DEBUG_FTL
    return;
}

void buffer_print(struct ssd *ssd) {
    printf("Entered [%s]\n", __FUNCTION__);

    tAVLTree *buffer = ssd->wbuffer;
    printf("Write buffer:\n");
    printf("Buffer size = %u, buffer secs = %u\n", buffer->max_secs, buffer->secs_cnt);
    buffer_print_lru(buffer);

    buffer = ssd->rbuffer;
    printf("Read buffer:\n");
    printf("Buffer size = %u, buffer secs = %u\n", buffer->max_secs, buffer->secs_cnt);
    buffer_print_lru(buffer);

    return;
}