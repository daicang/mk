package mk

import "errors"

var (
	errNoPage = errors.New("No page")
)

type pager struct {
	// free pages, sort by pgid
	freelist []pgid

	// pages to be freed by the end of transaction
	pending map[txid][]pgid
}

// Allocate contiguous pages from freelist,
// when can't, an error is returned
func (p *pager) allocate(n int) (pgid, error) {
	startPgid := pgid(0)
	lastPgid := pgid(0)

	for i, pid := range p.freelist {
		if i == 0 || pid != lastPgid+1 {
			startPgid = pid
		}

		if int(pid-startPgid+1) == n {

			// remove n contiguous pages
			copy(p.freelist[i-n+1:], p.freelist[i+1:])
			p.freelist = p.freelist[:len(p.freelist)-n]

			return startPgid, nil
		}

		lastPgid = pid
	}

	return 0, errNoPage
}

// free puts page to transaction pending pages
func (p *pager) free(ti txid, pg *page) {
	if pg.id <= 0 {
		panic("Page already freed")
	}

	for i := 0; i <= pg.overflow; i++ {
		p.pending[ti] = append(p.pending[ti], pg.id+pgid(i))
	}
}

func (p *pager) release(ti txid) {

}

// rollback returns pending page
func (p *pager) rollback(ti txid) {
	delete(p.pending, ti)
}
