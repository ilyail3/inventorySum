package sum

import (
	"fmt"
	"regexp"
)

type accountHolder struct {
	Account string
}

func (as accountHolder) String() string {
	return fmt.Sprintf("%s", as.Account)
}

type accountMapFunction struct {
	reg *regexp.Regexp
}

func (amf *accountMapFunction) mapRecord(record []string) (interface{}, error) {
	result := amf.reg.FindStringSubmatch(record[1])

	if len(result) == 0 {
		return nil, nil
	}

	return accountHolder{result[1]}, nil
}

func AccountMapFunction(month bool) (MapInterface, error) {
	var reg *regexp.Regexp = nil
	var err error = nil

	if month {
		reg, err = regexp.Compile("^accounts/([0-9]{12})/month/([0-9]{4})/([0-9]{2})\\.")
	} else {
		reg, err = regexp.Compile("^accounts/([0-9]{12})/day/([0-9]{4})/([0-9]{2})/")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to compile filename regex:%v", err)
	}

	return &accountMapFunction{reg: reg}, nil
}
