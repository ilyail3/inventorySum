package sum

import (
	"fmt"
	"regexp"
)

type accountStruct struct {
	Account string
}

func (as accountStruct) String() string {
	return fmt.Sprintf("<%s>", as.Account)
}

func AccountMapFunction()(func(record []string)(interface{},error), error){
	reg, err := regexp.Compile("^accounts/([0-9]{12})/day/([0-9]{4})/([0-9]{2})/")

	if err != nil {
		return nil, fmt.Errorf("failed to compile filename regex:%v", err)
	}

	return func(record []string)(interface{},error){
		result := reg.FindStringSubmatch(record[1])

		if len(result) == 0 {
			return nil, nil
		}

		return accountStruct{ result[1] }, nil
	}, nil
}

