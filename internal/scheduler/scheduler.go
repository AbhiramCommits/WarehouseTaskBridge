// Package scheduler sequences internal tasks onto a pool of bots using a
// deterministic priority, zone-affinity, and travel-cost model.
package scheduler

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/translate"
)

// Cost weights. Lower cost is better.
const (
	// WeightPriority scales the numeric priority (lower priority number is
	// more urgent, so urgency dominates travel).
	WeightPriority = 1.0
	// WeightTravel scales Manhattan travel distance.
	WeightTravel = 0.5
	// ZoneMismatchPenalty pushes a task toward a bot already in its zone.
	ZoneMismatchPenalty = 100.0
	// UnknownSlotPenalty makes an unparseable address effectively unusable
	// without panicking.
	UnknownSlotPenalty = 1_000_000.0
	// LevelWeight makes vertical travel more expensive than horizontal travel.
	LevelWeight = 2.0
)

// Bot is a fleet member available for assignment.
type Bot struct {
	ID        string
	Zone      string
	HomeSlot  string
	BusyUntil time.Time
	Status    string
}

// Available reports whether the bot can take work at now.
func (b Bot) Available(now time.Time) bool {
	if b.Status == "OFFLINE" {
		return false
	}
	return !b.BusyUntil.After(now)
}

// Assignment is one scheduler decision with its cost breakdown.
type Assignment struct {
	Task         *taskpb.Task
	Bot          Bot
	Cost         float64
	PriorityCost float64
	TravelCost   float64
	ZonePenalty  float64
	Reason       string
}

// Explain renders an auditable one-line explanation of an assignment.
func Explain(a Assignment) string {
	return fmt.Sprintf("task %s (order %s, priority %d) -> bot %s: cost %.2f [priority %.2f + travel %.2f + zone %.2f]",
		a.Task.GetId(), a.Task.GetOrderRef(), a.Task.GetPriority(), a.Bot.ID,
		a.Cost, a.PriorityCost, a.TravelCost, a.ZonePenalty)
}

// TravelCost is the weighted Manhattan distance between two slot strings over
// (aisle, bay, level), with level weighted more heavily.
func TravelCost(from, to string) float64 {
	a, err := translate.ParseSlot(from)
	if err != nil {
		return UnknownSlotPenalty
	}
	b, err := translate.ParseSlot(to)
	if err != nil {
		return UnknownSlotPenalty
	}
	return math.Abs(float64(a.Aisle-b.Aisle)) +
		math.Abs(float64(a.Bay-b.Bay)) +
		LevelWeight*math.Abs(float64(a.Level-b.Level))
}

// ZonePenalty returns the penalty for serving a task zone from a bot zone.
func ZonePenalty(botZone, taskZone string) float64 {
	if botZone == "" || taskZone == "" {
		return ZoneMismatchPenalty / 4
	}
	if botZone != taskZone {
		return ZoneMismatchPenalty
	}
	return 0
}

// Cost is the assignment cost of a task on a bot. Lower is better.
func Cost(t *taskpb.Task, b Bot) float64 {
	priorityCost := float64(t.GetPriority()) * WeightPriority
	travel := TravelCost(b.HomeSlot, t.GetSourceSlot()) * WeightTravel
	zone := ZonePenalty(b.Zone, t.GetZone())
	return priorityCost + travel + zone
}

// Assign greedily pairs pending tasks with available bots.
//
// Ordering is fully deterministic: tasks are considered by ascending priority,
// then ascending created_at, then lexicographic id. Each task takes the
// cheapest unused bot, with ties broken by lexicographic bot id. A bot is used
// at most once per call.
func Assign(ctx context.Context, tasks []*taskpb.Task, bots []Bot, now time.Time) []Assignment {
	if ctx.Err() != nil {
		return nil
	}
	orderedTasks := append([]*taskpb.Task(nil), tasks...)
	sort.SliceStable(orderedTasks, func(i, j int) bool {
		a, b := orderedTasks[i], orderedTasks[j]
		if a.GetPriority() != b.GetPriority() {
			return a.GetPriority() < b.GetPriority()
		}
		at, bt := createdTime(a), createdTime(b)
		if !at.Equal(bt) {
			return at.Before(bt)
		}
		return a.GetId() < b.GetId()
	})

	available := make([]Bot, 0, len(bots))
	for _, bot := range bots {
		if bot.Available(now) {
			available = append(available, bot)
		}
	}
	sort.Slice(available, func(i, j int) bool { return available[i].ID < available[j].ID })

	used := make([]bool, len(available))
	assignments := make([]Assignment, 0, min(len(orderedTasks), len(available)))
	for _, task := range orderedTasks {
		best := -1
		bestCost := math.MaxFloat64
		for i, bot := range available {
			if used[i] {
				continue
			}
			c := Cost(task, bot)
			if c < bestCost || (c == bestCost && (best < 0 || bot.ID < available[best].ID)) {
				best = i
				bestCost = c
			}
		}
		if best < 0 {
			break
		}
		used[best] = true
		bot := available[best]
		assignment := Assignment{
			Task:         task,
			Bot:          bot,
			Cost:         bestCost,
			PriorityCost: float64(task.GetPriority()) * WeightPriority,
			TravelCost:   TravelCost(bot.HomeSlot, task.GetSourceSlot()) * WeightTravel,
			ZonePenalty:  ZonePenalty(bot.Zone, task.GetZone()),
			Reason:       "cheapest available bot under priority/travel/zone cost",
		}
		assignments = append(assignments, assignment)
	}
	return assignments
}

// createdTime safely extracts a task creation time.
func createdTime(t *taskpb.Task) time.Time {
	if ts := t.GetCreatedAt(); ts != nil {
		return ts.AsTime()
	}
	return time.Time{}
}
