package sum

import (
	"fmt"
	"regexp"
	"strconv"
)

type yearMonth struct {
	Year int
	Month int
}

func (ym yearMonth) String() string {
	return fmt.Sprintf("<%d/%02d>", ym.Year, ym.Month)
}

func YearMonthMapFunction()(func(record []string)(interface{},error), error){
	reg, err := regexp.Compile("^accounts/([0-9]{12})/day/([0-9]{4})/([0-9]{2})/")

	if err != nil {
		return nil, fmt.Errorf("failed to compile filename regex:%v", err)
	}

	return func(record []string)(interface{},error){
		result := reg.FindStringSubmatch(record[1])

		if len(result) == 0 {
			return nil, nil
		}

		year,err := strconv.Atoi(result[2])

		if err != nil {
			return nil, fmt.Errorf("failed to parse year:%v", err)
		}

		month, err := strconv.Atoi(result[3])

		if err != nil {
			return nil, fmt.Errorf("failed to parse month:%v", err)
		}

		return yearMonth{
			Year: year,
			Month: month }, nil
	}, nil
}
