package internal

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeBlock(t *testing.T) {
	blockCh := time.After(time.Duration(1000) * time.Millisecond)
	<-blockCh
	fmt.Println("Done blocking")
	t.Log("Done")
}
