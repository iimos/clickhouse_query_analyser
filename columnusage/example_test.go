package columnusage_test

import (
	"fmt"
	"github.com/iimos/clickhouse_query_analyser/columnusage"
)

func Example() {
	schema := []columnusage.Column{
		{"system", "numbers", "number"},
		{"system", "clusters", "cluster"},
		{"system", "clusters", "shard_num"},
		{"system", "clusters", "shard_weight"},
		{"system", "clusters", "replica_num"},
		{"system", "clusters", "host_name"},
		{"system", "clusters", "host_address"},
		{"system", "clusters", "port"},
		{"system", "clusters", "is_local"},
		{"system", "clusters", "user"},
		{"system", "clusters", "default_database"},
		{"system", "clusters", "errors_count"},
		{"system", "clusters", "estimated_recovery_time"},
		// ...
	}
	analyser := columnusage.NewColumnUsageAnalyser(schema)
	query := `
select clu.host_name as hostName, coalesce(val.value, 0) as value
from system.clusters as clu
left join (
    select fullHostName() as hostName, sum(rows) as value
    from cluster('shards', system, part_log)
    where Database = 'raw'
      and table = 'events'
      and event_type = 'NewPart'
      and event_time between subtractMinutes(now(), 5) and now()
    group by hostName
) as val on clu.host_name = val.hostName
where clu.cluster = 'shards'
`
	res, err := analyser.ParseQuery("default", query)
	if err != nil {
		// ...
	}
	fmt.Printf("%v", res)
}
