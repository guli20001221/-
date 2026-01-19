package Group_Cache

import (
	"os"

	"github.com/sirupsen/logrus"
)

// init routes logrus output to stdout for easier log capture.
func init() {
	logrus.SetOutput(os.Stdout)
}
