package miner

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	TypeMinerLittle   MinerType = "little"
	LittleMinerSalary int       = 5
)

type LittleMiner struct {
	id      uuid.UUID
	energy  *atomic.Int64
	power   *atomic.Int64
	timeout time.Duration
}

func NewLittleMiner() *LittleMiner {
	const (
		littleMinerEnergy   = 30
		littleMinerInterval = 3 * time.Second
		littleMinerPower    = 1
	)

	energy := &atomic.Int64{}
	power := &atomic.Int64{}

	energy.Add(littleMinerEnergy)
	power.Add(littleMinerPower)

	return &LittleMiner{
		id:      uuid.New(),
		energy:  energy,
		power:   power,
		timeout: littleMinerInterval,
	}
}

func (lm *LittleMiner) Run(ctx context.Context) <-chan Coal {
	transferPoint := make(chan Coal)

	go func() {
		defer func() {
			close(transferPoint)
			fmt.Println("little miner stopped:", lm.id)
		}()

		fmt.Println("little miner started:", lm.id)

		for {
			select {
			case <-ctx.Done():
				return

			case <-time.After(lm.timeout):
			}

			select {
			case <-ctx.Done():
				return

			case transferPoint <- Coal(lm.power.Load()):
				if new := lm.energy.Add(-1); new <= 0 {
					return
				}
			}

		}
	}()

	return transferPoint
}

func (lm *LittleMiner) Info() MinerInfo {
	return MinerInfo{
		ID:          lm.id,
		MinerType:   TypeMinerLittle,
		MinerEnergy: lm.energy.Load(),
		MinerPower:  lm.power.Load(),
	}
}
