package mk

import "testing"

func TestPageBuffer(t *testing.T) {
	buf := make([]byte, PageSize)
	p := FromBuffer(buf, 0)
	p.Index = 46
	p.Count = 42

	getPgid := FromBuffer(buf, 0).Index
	if getPgid != 46 {
		t.Errorf("pgid expect 46, get %d", getPgid)
	}

	getKeyCount := FromBuffer(buf, 0).Count
	if getKeyCount != 42 {
		t.Errorf("count expect 42, get %d", getKeyCount)
	}

	p.SetFlag(FlagMeta)
	isMeta := FromBuffer(buf, 0).IsMeta()
	if !isMeta {
		t.Error("Page should be meta")
	}

	p.SetFlag(FlagFreelist)
	isFreelist := FromBuffer(buf, 0).IsFreelist()
	if !isFreelist {
		t.Error("Page should be freelist")
	}
}
