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

type yearMonthMapFunction struct {
	reg *regexp.Regexp
}

func (ymmf *yearMonthMapFunction) mapRecord(record []string)(interface{}, error) {
	result := ymmf.reg.FindStringSubmatch(record[1])

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
}

func YearMonthMapFunction()(MapInterface, error){
	reg, err := regexp.Compile("^accounts/([0-9]{12})/day/([0-9]{4})/([0-9]{2})/")

	if err != nil {
		return nil, fmt.Errorf("failed to compile filename regex:%v", err)
	}

	return &yearMonthMapFunction{ reg: reg }, nil
}
