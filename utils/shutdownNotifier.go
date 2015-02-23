package utils

import (
	//"log"
	"time"
)

/*
ShutdownNotifier provides a way of managing asynchronous shutdown requests of the Forest Bus sub-systems.
*/
type ShutdownNotifier struct {
	numRoutines       int
	confirmedShutdown int
	channel           chan interface{}
}

/*
NewShutdownNotifier provides a new ShutdownNotifier that is expecting numRoutines to notify shutdown completion through this object.
*/
func NewShutdownNotifier(numRoutines int) *ShutdownNotifier {
	return &ShutdownNotifier{numRoutines: numRoutines, channel: make(chan interface{}, numRoutines)}
}

/*
WaitForDone blocks until all of the expected routines have completed or the given timeout occurs.  The number of routines that actually notified of shutdown completion is returned.
*/
func (notifier *ShutdownNotifier) WaitForDone(timeout time.Duration) int {
	waitingForShutdown := true
	timer := time.NewTimer(timeout)
	//log.Printf("Waiting for %v items to shutdown with a timeout of %v\n", notifier.numRoutines, timeout)
	for waitingForShutdown && notifier.confirmedShutdown < notifier.numRoutines {
		select {
		case <-notifier.channel:
			notifier.confirmedShutdown++
		case <-timer.C:
			waitingForShutdown = false
		}
	}
	//log.Printf("Finished waiting - completed in timeout: %v, total done: %v\n", waitingForShutdown, notifier.confirmedShutdown)
	return notifier.confirmedShutdown
}

/*
WaitForAllDone blocks until all expected routines have reported that their shutdown is done.
*/
func (notifier *ShutdownNotifier) WaitForAllDone() {
	for notifier.confirmedShutdown < notifier.numRoutines {
		<-notifier.channel
		notifier.confirmedShutdown++
	}
}

/*
ShutdownDone is used by a routine to inform the ShutdownNotifier that is has completed shutdown.
*/
func (notifier *ShutdownNotifier) ShutdownDone() {
	notifier.channel <- struct{}{}
}
