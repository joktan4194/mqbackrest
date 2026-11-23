package repo

import "sort"

// ExpirePlan is what `mqbackrest expire` will delete.
type ExpirePlan struct {
	Backups []string // labels, oldest first
	Binlogs []string // binlog names no retained backup needs
}

// PlanExpire keeps the newest keepFull full backups plus every incremental
// built on them, and the binlogs needed to roll forward from the oldest kept backup.
func PlanExpire(backups []*Manifest, binlogs []*BinlogMeta, keepFull int) ExpirePlan {
	var fulls []*Manifest
	for _, b := range backups {
		if b.Type == TypeFull {
			fulls = append(fulls, b)
		}
	}
	sort.Slice(fulls, func(i, j int) bool { return fulls[i].StartTime.Before(fulls[j].StartTime) })
	keep := map[string]bool{}
	for i := max(0, len(fulls)-keepFull); i < len(fulls); i++ {
		keep[fulls[i].Label] = true
	}

	var plan ExpirePlan
	var oldestKept *Manifest
	for _, b := range backups {
		if keep[b.Root()] {
			if oldestKept == nil || b.StartTime.Before(oldestKept.StartTime) {
				oldestKept = b
			}
			continue
		}
		plan.Backups = append(plan.Backups, b.Label)
	}

	if oldestKept == nil || oldestKept.Binlog == nil {
		return plan // nothing anchors the archive; keep every binlog
	}
	cut, ok := ParseBinlogName(oldestKept.Binlog.File)
	if !ok {
		return plan
	}
	for _, bl := range binlogs {
		n, ok := ParseBinlogName(bl.Name)
		if ok && n.Base == cut.Base && n.Seq < cut.Seq {
			plan.Binlogs = append(plan.Binlogs, bl.Name)
		}
	}
	return plan
}
