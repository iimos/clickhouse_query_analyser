package columnusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var realSchema = []Column{
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

	{"system", "part_log", "event_type"},
	{"system", "part_log", "event_date"},
	{"system", "part_log", "event_time"},
	{"system", "part_log", "duration_ms"},
	{"system", "part_log", "database"},
	{"system", "part_log", "table"},
	{"system", "part_log", "part_name"},
	{"system", "part_log", "partition_id"},
	{"system", "part_log", "path_on_disk"},
	{"system", "part_log", "rows"},
	{"system", "part_log", "size_in_bytes"},
	{"system", "part_log", "merged_from"},
	{"system", "part_log", "bytes_uncompressed"},
	{"system", "part_log", "read_rows"},
	{"system", "part_log", "read_bytes"},
	{"system", "part_log", "peak_memory_usage"},
	{"system", "part_log", "error"},
	{"system", "part_log", "exception"},

	{"tracker", "events", "action_area"},
	{"tracker", "events", "action_context"},
	{"tracker", "events", "action_object"},
	{"tracker", "events", "action_type"},
	{"tracker", "events", "action_value"},
	{"tracker", "events", "action_widget"},
	{"tracker", "events", "appsflyer_campaign"},
	{"tracker", "events", "appsflyer_is_retargeting"},
	{"tracker", "events", "appsflyer_media_source"},
	{"tracker", "events", "attributes_ab_group"},
	{"tracker", "events", "attributes_advertising"},
	{"tracker", "events", "attributes_advertising_id"},
	{"tracker", "events", "attributes_app_version"},
	{"tracker", "events", "attributes_device_type"},
	{"tracker", "events", "attributes_domain_name"},
	{"tracker", "events", "attributes_mobile_device_branding"},
	{"tracker", "events", "attributes_mobile_device_model"},
	{"tracker", "events", "attributes_namespace"},
	{"tracker", "events", "attributes_os_version"},
	{"tracker", "events", "attributes_page"},
	{"tracker", "events", "attributes_platform"},
	{"tracker", "events", "attributes_platform_store"},
	{"tracker", "events", "attributes_previous_page"},
	{"tracker", "events", "attributes_screen_resolution_x"},
	{"tracker", "events", "attributes_screen_resolution_y"},
	{"tracker", "events", "attributes_template_type"},
	{"tracker", "events", "attributes_user_agent"},
	{"tracker", "events", "backend_timestamp"},
	{"tracker", "events", "custom_dimension1"},
	{"tracker", "events", "custom_dimension10"},
	{"tracker", "events", "custom_dimension2"},
	{"tracker", "events", "custom_dimension3"},
	{"tracker", "events", "custom_dimension4"},
	{"tracker", "events", "custom_dimension5"},
	{"tracker", "events", "custom_dimension6"},
	{"tracker", "events", "custom_dimension7"},
	{"tracker", "events", "custom_dimension8"},
	{"tracker", "events", "custom_dimension9"},
	{"tracker", "events", "date"},
	{"tracker", "events", "etm_campaign"},
	{"tracker", "events", "etm_content"},
	{"tracker", "events", "etm_medium"},
	{"tracker", "events", "etm_source"},
	{"tracker", "events", "etm_term"},
	{"tracker", "events", "etm_tsdv"},
	{"tracker", "events", "insert_timestamp"},
	{"tracker", "events", "micro_sec"},
	{"tracker", "events", "number"},
	{"tracker", "events", "object_banner_id"},
	{"tracker", "events", "object_brand_id"},
	{"tracker", "events", "object_cart_type"},
	{"tracker", "events", "object_category_id"},
	{"tracker", "events", "object_delivery_service"},
	{"tracker", "events", "object_delivery_type"},
	{"tracker", "events", "object_filter_id"},
	{"tracker", "events", "object_id"},
	{"tracker", "events", "object_iid"},
	{"tracker", "events", "object_marketing_action_id"},
	{"tracker", "events", "object_menu_id"},
	{"tracker", "events", "object_message_id"},
	{"tracker", "events", "object_order_id"},
	{"tracker", "events", "object_payment_type"},
	{"tracker", "events", "object_posting_id"},
	{"tracker", "events", "object_promocode"},
	{"tracker", "events", "object_region_id"},
	{"tracker", "events", "object_review_id"},
	{"tracker", "events", "object_search_string"},
	{"tracker", "events", "object_seller_id"},
	{"tracker", "events", "object_shipment_id"},
	{"tracker", "events", "object_sku"},
	{"tracker", "events", "object_sort_id"},
	{"tracker", "events", "object_suggest_type"},
	{"tracker", "events", "object_type"},
	{"tracker", "events", "object_virtual_posting_id"},
	{"tracker", "events", "page_brand_id"},
	{"tracker", "events", "page_category_id"},
	{"tracker", "events", "page_composer_page_type"},
	{"tracker", "events", "page_current"},
	{"tracker", "events", "page_current_url"},
	{"tracker", "events", "page_highlight_id"},
	{"tracker", "events", "page_layout_id"},
	{"tracker", "events", "page_layout_version"},
	{"tracker", "events", "page_next_url"},
	{"tracker", "events", "page_number"},
	{"tracker", "events", "page_previous"},
	{"tracker", "events", "page_referral_url"},
	{"tracker", "events", "page_rule_id"},
	{"tracker", "events", "page_search_query"},
	{"tracker", "events", "page_seller_id"},
	{"tracker", "events", "page_sku"},
	{"tracker", "events", "page_tag_id"},
	{"tracker", "events", "page_transition"},
	{"tracker", "events", "page_transition_identifier"},
	{"tracker", "events", "page_transition_widget"},
	{"tracker", "events", "page_view_id"},
	{"tracker", "events", "pod_ip"},
	{"tracker", "events", "previous_page_view_id"},
	{"tracker", "events", "properties_additional_quantity"},
	{"tracker", "events", "properties_address"},
	{"tracker", "events", "properties_address_book_id"},
	{"tracker", "events", "properties_address_uid"},
	{"tracker", "events", "properties_adv_id"},
	{"tracker", "events", "properties_adv_second_bid"},
	{"tracker", "events", "properties_advertising"},
	{"tracker", "events", "properties_algo"},
	{"tracker", "events", "properties_aspect"},
	{"tracker", "events", "properties_aspect_value"},
	{"tracker", "events", "properties_author_id"},
	{"tracker", "events", "properties_author_type"},
	{"tracker", "events", "properties_available_delivery_schema"},
	{"tracker", "events", "properties_brand_id"},
	{"tracker", "events", "properties_brand_name"},
	{"tracker", "events", "properties_bundle_id"},
	{"tracker", "events", "properties_card_id"},
	{"tracker", "events", "properties_card_is_remember"},
	{"tracker", "events", "properties_card_type"},
	{"tracker", "events", "properties_cart_type"},
	{"tracker", "events", "properties_category_id"},
	{"tracker", "events", "properties_chat_id"},
	{"tracker", "events", "properties_chat_type"},
	{"tracker", "events", "properties_checked"},
	{"tracker", "events", "properties_checkout_session_id"},
	{"tracker", "events", "properties_component_id"},
	{"tracker", "events", "properties_context"},
	{"tracker", "events", "properties_count_items"},
	{"tracker", "events", "properties_count_postings"},
	{"tracker", "events", "properties_country_id"},
	{"tracker", "events", "properties_credit_product_price"},
	{"tracker", "events", "properties_credit_product_type"},
	{"tracker", "events", "properties_cut_off"},
	{"tracker", "events", "properties_date"},
	{"tracker", "events", "properties_dateslots_all"},
	{"tracker", "events", "properties_delivery_marketing_action_id"},
	{"tracker", "events", "properties_delivery_point_id"},
	{"tracker", "events", "properties_delivery_provider_id"},
	{"tracker", "events", "properties_delivery_service"},
	{"tracker", "events", "properties_delivery_service_first_date_available"},
	{"tracker", "events", "properties_delivery_service_id"},
	{"tracker", "events", "properties_delivery_service_id_available"},
	{"tracker", "events", "properties_delivery_service_id_first_time_available"},
	{"tracker", "events", "properties_delivery_service_price_available"},
	{"tracker", "events", "properties_delivery_time_diff_days"},
	{"tracker", "events", "properties_delivery_type"},
	{"tracker", "events", "properties_department"},
	{"tracker", "events", "properties_discount"},
	{"tracker", "events", "properties_due"},
	{"tracker", "events", "properties_element_type"},
	{"tracker", "events", "properties_filter_value"},
	{"tracker", "events", "properties_final_delivery_price"},
	{"tracker", "events", "properties_final_price"},
	{"tracker", "events", "properties_first_available_cut_off"},
	{"tracker", "events", "properties_first_available_date"},
	{"tracker", "events", "properties_first_available_timeslot"},
	{"tracker", "events", "properties_from_aboard"},
	{"tracker", "events", "properties_is_available"},
	{"tracker", "events", "properties_is_call_me"},
	{"tracker", "events", "properties_is_delivery_service_selected_by_user"},
	{"tracker", "events", "properties_is_exact"},
	{"tracker", "events", "properties_is_fast_delivery"},
	{"tracker", "events", "properties_is_geo_allowed"},
	{"tracker", "events", "properties_is_in_storage"},
	{"tracker", "events", "properties_is_leave_at_door"},
	{"tracker", "events", "properties_is_own"},
	{"tracker", "events", "properties_is_personalized"},
	{"tracker", "events", "properties_is_supermarket"},
	{"tracker", "events", "properties_is_time_selected_by_user"},
	{"tracker", "events", "properties_latitude"},
	{"tracker", "events", "properties_longitude"},
	{"tracker", "events", "properties_mail"},
	{"tracker", "events", "properties_marketing_action_id"},
	{"tracker", "events", "properties_marketing_action_ids"},
	{"tracker", "events", "properties_menu_id"},
	{"tracker", "events", "properties_new"},
	{"tracker", "events", "properties_number"},
	{"tracker", "events", "properties_order_id"},
	{"tracker", "events", "properties_order_unavailable"},
	{"tracker", "events", "properties_quantity_scores"},
	{"tracker", "events", "properties_rating"},
	{"tracker", "events", "properties_reason"},
	{"tracker", "events", "properties_region_id"},
	{"tracker", "events", "properties_regular"},
	{"tracker", "events", "properties_restriction_type"},
	{"tracker", "events", "properties_review_version"},
	{"tracker", "events", "properties_scores"},
	{"tracker", "events", "properties_search_string"},
	{"tracker", "events", "properties_selected_delivery_schema"},
	{"tracker", "events", "properties_seller_id"},
	{"tracker", "events", "properties_shipment_id"},
	{"tracker", "events", "properties_shipments"},
	{"tracker", "events", "properties_sku"},
	{"tracker", "events", "properties_split_reason"},
	{"tracker", "events", "properties_timeslots_count"},
	{"tracker", "events", "properties_title"},
	{"tracker", "events", "properties_total_quantity"},
	{"tracker", "events", "properties_type_scores"},
	{"tracker", "events", "properties_unavailable_reason"},
	{"tracker", "events", "properties_url"},
	{"tracker", "events", "properties_video_timescale_from"},
	{"tracker", "events", "properties_video_timescale_to"},
	{"tracker", "events", "properties_virtual_posting_id"},
	{"tracker", "events", "properties_weight"},
	{"tracker", "events", "server_timestamp"},
	{"tracker", "events", "timestamp"},
	{"tracker", "events", "timezone"},
	{"tracker", "events", "user_ab_group"},
	{"tracker", "events", "user_active_ticket_id"},
	{"tracker", "events", "user_app_build_number"},
	{"tracker", "events", "user_appsflyer_id"},
	{"tracker", "events", "user_browser"},
	{"tracker", "events", "user_browser_version"},
	{"tracker", "events", "user_client_id"},
	{"tracker", "events", "user_company_id"},
	{"tracker", "events", "user_firebase_install_id"},
	{"tracker", "events", "user_full_name"},
	{"tracker", "events", "user_google_id"},
	{"tracker", "events", "user_ip"},
	{"tracker", "events", "user_is_bot"},
	{"tracker", "events", "user_is_premium"},
	{"tracker", "events", "user_latitude"},
	{"tracker", "events", "user_long_cookie"},
	{"tracker", "events", "user_longitude"},
	{"tracker", "events", "user_loyalty_current_level"},
	{"tracker", "events", "user_loyalty_is_active"},
	{"tracker", "events", "user_loyalty_is_available"},
	{"tracker", "events", "user_omniture_cookie"},
	{"tracker", "events", "user_os"},
	{"tracker", "events", "user_premium_subscription"},
	{"tracker", "events", "user_region_id"},
	{"tracker", "events", "user_role"},
	{"tracker", "events", "user_sberbank_id"},
	{"tracker", "events", "user_session_id"},
	{"tracker", "events", "user_tracker_id"},
	{"tracker", "events", "user_visit_id"},
	{"tracker", "events", "user_visit_number"},
	{"tracker", "events", "user_visit_start"},
	{"tracker", "events", "user_yandex_id"},
	{"tracker", "events", "utm_campaign"},
	{"tracker", "events", "utm_content"},
	{"tracker", "events", "utm_medium"},
	{"tracker", "events", "utm_referral"},
	{"tracker", "events", "utm_source"},
	{"tracker", "events", "utm_term"},
	{"tracker", "events", "utm_tsdv"},
	{"tracker", "events", "uuid"},
	{"tracker", "events", "version"},
	{"tracker", "events", "widget_config_dt_id"},
	{"tracker", "events", "widget_config_id"},
	{"tracker", "events", "widget_config_revision_id"},
	{"tracker", "events", "widget_dt_name"},
	{"tracker", "events", "widget_id"},
	{"tracker", "events", "widget_index"},
	{"tracker", "events", "widget_name"},
	{"tracker", "events", "widget_origin_name"},
	{"tracker", "events", "widget_revision_id"},
	{"tracker", "events", "widget_slice_algorithm"},
	{"tracker", "events", "widget_slice_id"},
	{"tracker", "events", "widget_slice_index"},
	{"tracker", "events", "widget_slice_search_string"},
	{"tracker", "events", "widget_slice_string"},
	{"tracker", "events", "widget_slice_title"},
	{"tracker", "events", "widget_slice_type"},
	{"tracker", "events", "widget_time_spent"},
	{"tracker", "events", "widget_type"},
	{"tracker", "events", "widget_version"},
	{"tracker", "events", "properties_items.available_delivery_schema"},
	{"tracker", "events", "properties_items.available_quantity"},
	{"tracker", "events", "properties_items.category_id"},
	{"tracker", "events", "properties_items.checked"},
	{"tracker", "events", "properties_items.delivery_type"},
	{"tracker", "events", "properties_items.final_price"},
	{"tracker", "events", "properties_items.is_available"},
	{"tracker", "events", "properties_items.is_fast_delivery"},
	{"tracker", "events", "properties_items.is_services_available"},
	{"tracker", "events", "properties_items.is_supermarket"},
	{"tracker", "events", "properties_items.marketing_action_ids"},
	{"tracker", "events", "properties_items.original_price"},
	{"tracker", "events", "properties_items.quantity"},
	{"tracker", "events", "properties_items.selected_delivery_schema"},
	{"tracker", "events", "properties_items.seller_id"},
	{"tracker", "events", "properties_items.sku"},
	{"tracker", "events", "properties_items.stock"},
	{"tracker", "events", "properties_items.storehouse_id"},
	{"tracker", "events", "properties_items.unavailable_reason"},
	{"tracker", "events", "properties_items.unavailable_tags"},
	{"tracker", "events", "properties_labels.names"},
	{"tracker", "events", "properties_objects.delivery_type"},
	{"tracker", "events", "properties_objects.free_delivery_amount"},
	{"tracker", "events", "properties_objects.free_delivery_amount_lack"},
	{"tracker", "events", "properties_objects.min_order_amount"},
	{"tracker", "events", "properties_objects.min_order_amount_lack"},
	{"tracker", "events", "properties_objects.type"},
	{"tracker", "events", "properties_objects.url"},
	{"tracker", "events", "user_experiments.id"},
	{"tracker", "events", "user_experiments.variant"},
}

//nolint:dupl
func TestParseQuery(t *testing.T) {
	tests := []struct {
		name   string
		db     string
		sql    string
		schema []Column
		want   Stat
		error  bool
	}{
		{
			name: "const",
			sql:  "select 1, 'some str'",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "1"},
					{Name: "'some str'"}, // todo: maybe its not how it should be
				},
				ColumnUsages: []ColumnUsage{},
			},
		},
		{
			name: "simple",
			sql:  "select aaa as a, bbb, ccc from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "a", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "simple with column prefixes",
			sql:  "select aaa, test.bbb, db.test.ccc from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Scope: "", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Scope: "", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Scope: "", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "simple with asterisk",
			sql:  "select * from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select", Implicit: true,
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select", Implicit: true,
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select", Implicit: true,
					},
				},
			},
		},
		{
			name: "simple with asterisk 2",
			sql:  "select test.* from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select", Implicit: true,
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select", Implicit: true,
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select", Implicit: true,
					},
				},
			},
		},
		{
			name: "quotes",
			sql:  "select test.`aaa`, `test`.`bbb`, `ccc`, \"ddd\" from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
				{"db", "test", "ddd"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
					{Name: "ddd", Columns: []Column{{"db", "test", "ddd"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ddd"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "quote escaping",
			sql:  "select 1 as `aa``bb`, 1 as \"aa\"\"bb\", 1 as `aa\"\"bb`, 1 as \"aa``bb\"",
			want: Stat{
				Export: []Expression{
					{Name: "aa`bb"},
					{Name: "aa\"bb"},
					{Name: "aa\"\"bb"},
					{Name: "aa``bb"},
				},
				ColumnUsages: []ColumnUsage{},
			},
		},
		{
			name: "simple with comparison",
			sql:  "select 1 from test where aaa = 123",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "1"},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column:         Column{Database: "db", Table: "test", Name: "aaa"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "123",
					},
				},
			},
		},
		{
			name: "functions",
			sql:  "select avg(aaa), max(`test`.`bbb`), groupUniq(10)(`ccc`) from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "avg(aaa)", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "max(`test`.`bbb`)", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "groupUniq(10)(`ccc`)", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "duplicates in export",
			sql:  "select bbb_alias, aaa, aaa, aaa as xxx, xxx from test where (bbb as bbb_alias) = 123",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "bbb_alias", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "xxx", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "xxx", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "where",
					},
				},
			},
		},
		{
			name: "alias based on another aliases",
			sql:  "select alias3 from test where (alias2 as alias3) = 123 order by alias1 as alias2, aaa as alias1",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "alias3", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "where",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "order by",
					},
				},
			},
		},
		{
			name: "nested",
			sql:  "select nest.a, `nest.b` from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "nest.a"},
				{"db", "test", "nest.b"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "nest.a", Columns: []Column{{"db", "test", "nest.a"}}},
					{Name: "nest.b", Columns: []Column{{"db", "test", "nest.b"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "nest.a"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "nest.b"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "alias",
			sql:  "select x.aaa, bbb as x, x from test as x",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "x", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "x", Columns: []Column{{"db", "test", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "subselect with alias",
			sql:  "select x.aaa from (select * from test) as x",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "subselect without alias",
			sql:  "select aaa from (select * from test)",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "subselect in from",
			sql:  "select ccc from (select aaa + bbb as ccc from test)",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "ccc", Columns: []Column{{"db", "test", "aaa"}, {"db", "test", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "subselect in where",
			sql:  "select val from t1 where id in (select id from t2)",
			db:   "db",
			schema: []Column{
				{"db", "t1", "id"},
				{"db", "t1", "val"},
				{"db", "t2", "id"},
				{"db", "t2", "val"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "val", Columns: []Column{{"db", "t1", "val"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t2", Name: "id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "val"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "id"}, Purpose: "where",
					},
				},
			},
		},
		{
			name: "multiple anon subselects",
			sql:  "select val from t1 where id in (select id from t2) and id not in (select id from t3)",
			db:   "db",
			schema: []Column{
				{"db", "t1", "id"},
				{"db", "t1", "val"},
				{"db", "t2", "id"},
				{"db", "t2", "val"},
				{"db", "t3", "id"},
				{"db", "t3", "val"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "val", Columns: []Column{{"db", "t1", "val"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t2", Name: "id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t3", Name: "id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "val"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "id"}, Purpose: "where",
					},
				},
			},
		},
		{
			name: "select select",
			sql:  "select 1 as one, (select uniq(bbb) from test) as aaa",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "one"},
					{Name: "aaa", Columns: []Column{{"db", "test", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "nested subselects",
			sql: `select xxx.* from (
				select val from t1 where id in (select id from t2)
			) as xxx`,
			db: "db",
			schema: []Column{
				{"db", "t1", "id"},
				{"db", "t1", "val"},
				{"db", "t2", "id"},
				{"db", "t2", "val"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "val", Columns: []Column{{"db", "t1", "val"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t2", Name: "id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "val"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "id"}, Purpose: "where",
					},
				},
			},
		},
		{
			name: "where + group by",
			sql:  "select max(aaa) from test sample 1/100 prewhere bbb='123' where ccc=`ddd` group by eee having fff > 0",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
				{"db", "test", "ddd"},
				{"db", "test", "eee"},
				{"db", "test", "fff"},
				{"db", "test", "ggg"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "max(aaa)", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column:  Column{Database: "db", Table: "test", Name: "aaa"},
						Purpose: "select",
					},
					{
						Column:         Column{Database: "db", Table: "test", Name: "bbb"},
						Purpose:        "prewhere",
						ComparisonOp:   "=",
						ComparisonWith: "'123'",
					},
					{
						Column:  Column{Database: "db", Table: "test", Name: "ccc"},
						Purpose: "where",
					},
					{
						Column:  Column{Database: "db", Table: "test", Name: "ddd"},
						Purpose: "where",
					},
					{
						Column:  Column{Database: "db", Table: "test", Name: "eee"},
						Purpose: "group by",
					},
					{
						Column:         Column{Database: "db", Table: "test", Name: "fff"},
						Purpose:        "having",
						ComparisonOp:   ">",
						ComparisonWith: "0",
					},
				},
			},
		},
		{
			name: "join",
			sql:  "select f1, f2 from t1 left join t2 on t2.id2 = t1.id1",
			db:   "db",
			schema: []Column{
				{"db", "t1", "id1"},
				{"db", "t1", "f1"},
				{"db", "t2", "id2"},
				{"db", "t2", "f2"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "f1", Columns: []Column{{"db", "t1", "f1"}}},
					{Name: "f2", Columns: []Column{{"db", "t2", "f2"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t1", Name: "f1"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t2", Name: "f2"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t2", Name: "id2"}, Purpose: "from",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "id1"}, Purpose: "from",
					},
				},
			},
		},
		{
			name: "alias from subselect is not accessible for parent",
			sql: `select id, bbb_alias from t1
                   join (select aaa from t2 order by bbb as bbb_alias) using (id)`,
			error: true,
			db:    "db",
			schema: []Column{
				{"db", "t1", "id"},
				{"db", "t1", "val"},
				{"db", "t2", "id"},
				{"db", "t2", "val"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "val", Columns: []Column{{"db", "t1", "val"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t2", Name: "id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "val"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "id"}, Purpose: "where",
					},
				},
			},
		},
		{
			name: "alias has higher priority than column",
			sql:  "with 1 as aaa select aaa, bbb from t2",
			db:   "db",
			schema: []Column{
				{"db", "t2", "aaa"},
				{"db", "t2", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa"},
					{Name: "bbb", Columns: []Column{{"db", "t2", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t2", Name: "bbb"}, Purpose: "select",
					},
				},
			},
		},
		{
			name:  "regular literals dont become Columns",
			sql:   "select 1, `1` where 1 in (1, 2)",
			error: true,
		},
		{
			name: "tuple",
			sql:  "select aaa.1 from db.t1",
			db:   "db",
			schema: []Column{
				{"db", "t1", "aaa"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa.1", Columns: []Column{{Database: "db", Table: "t1", Name: "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t1", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "first table has higher priority",
			sql:  "select aaa from t1, t2",
			db:   "db",
			schema: []Column{
				{"db", "t2", "aaa"},
				{"db", "t1", "aaa"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "t1", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t1", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "alias has higher priority than column 2",
			sql:  "with (select uniq(i, j) FROM t1) as aaa select aaa, bbb from t2",
			db:   "db",
			schema: []Column{
				{"db", "t1", "i"},
				{"db", "t1", "j"},
				{"db", "t1", "k"},
				{"db", "t2", "aaa"},
				{"db", "t2", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{
						{"db", "t1", "i"},
						{"db", "t1", "j"},
					}},
					{Name: "bbb", Columns: []Column{{"db", "t2", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t1", Name: "i"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "j"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t2", Name: "bbb"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "limit by",
			sql:  "select sum(bbb) from t1 limit 100 by aaa",
			db:   "db",
			schema: []Column{
				{"db", "t1", "aaa"},
				{"db", "t1", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "sum(bbb)", Columns: []Column{{"db", "t1", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "t1", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "t1", Name: "aaa"}, Purpose: "limit by",
					},
				},
			},
		},

		{
			name: "insert select",
			sql:  "insert into test2 select aaa as a, bbb, ccc from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
				{"db", "test2", "aaa"},
				{"db", "test2", "bbb"},
				{"db", "test2", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "a", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
				},
			},
		},

		{
			name: "union",
			sql:  "select aaa, bbb, ccc from test union all select aaa, bbb, ccc from test2",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test", "ccc"},
				{"db", "test2", "aaa"},
				{"db", "test2", "bbb"},
				{"db", "test2", "ccc"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
					{Name: "bbb", Columns: []Column{{"db", "test", "bbb"}}},
					{Name: "ccc", Columns: []Column{{"db", "test", "ccc"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "ccc"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test2", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test2", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test2", Name: "ccc"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "cluster function",
			sql:  "select aaa from cluster('shards', db.test)",
			db:   "ddd",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "cluster function 2",
			sql:  "select aaa from cluster('shards', db, test)",
			db:   "ddd",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
				{"db", "test2", "aaa"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "cluster function 3",
			sql:  "select aaa from cluster('shards', 'db', 'test')",
			db:   "ddd",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "cluster function 4",
			sql:  "select aaa from cluster('shards', 'db.test')",
			db:   "ddd",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "aaa", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "lambda function",
			sql:  "select arrayMap(x -> lower(x), aaa) as lo from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "lo", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "lambda function: nested",
			sql:  "select arrayMap((x, i) -> arrayMap(y -> length(y)+length(x)+i, x), aaa, arrayEnumerate(aaa)) as res from test",
			db:   "db",
			schema: []Column{
				{"db", "test", "aaa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "res", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
				},
			},
		},
		{
			name: "array join",
			sql:  "select x, y, z from db.test array join `a.aa` as x, a.aa as y, `bbb` as z",
			db:   "db",
			schema: []Column{
				{"db", "test", "a.aa"},
				{"db", "test", "bbb"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "x", Columns: []Column{{"db", "test", "a.aa"}}},
					{Name: "y", Columns: []Column{{"db", "test", "a.aa"}}},
					{Name: "z", Columns: []Column{{"db", "test", "bbb"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "a.aa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "a.aa"}, Purpose: "from",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "bbb"}, Purpose: "from",
					},
				},
			},
		},
		{
			name: "alias chain",
			sql: `
select alias2 from (
    select alias1 alias2
    from (
        select aaa from db.test
    )
    order by aaa as alias1
)`,
			db: "db",
			schema: []Column{
				{"db", "test", "aaa"},
			},
			want: Stat{
				Export: []Expression{
					{Name: "alias2", Columns: []Column{{"db", "test", "aaa"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "select",
					},
					{
						Column: Column{Database: "db", Table: "test", Name: "aaa"}, Purpose: "order by",
					},
				},
			},
		},
		{
			name: "difficult case 1",
			sql: "SELECT `tracker`.`events`.attributes_namespace as `a.a.a`, `a.a.a`, user_session_id, action_type, " +
				"`a.a.a`.`properties_items.final_price`, properties_items.final_price " +
				"FROM tracker.`events` `a.a.a` " +
				"WHERE date=today() and attributes_namespace='bx' ",
			db:     "db",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "a.a.a", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "a.a.a", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "user_session_id", Columns: []Column{{"tracker", "events", "user_session_id"}}},
					{Name: "action_type", Columns: []Column{{"tracker", "events", "action_type"}}},
					{Name: "properties_items.final_price", Columns: []Column{{"tracker", "events", "properties_items.final_price"}}},
					{Name: "properties_items.final_price", Columns: []Column{{"tracker", "events", "properties_items.final_price"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column: Column{Database: "tracker", Table: "events", Name: "attributes_namespace"}, Purpose: "select",
					},
					{
						Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "select",
					},
					{
						Column: Column{Database: "tracker", Table: "events", Name: "action_type"}, Purpose: "select",
					},
					{
						Column: Column{Database: "tracker", Table: "events", Name: "properties_items.final_price"}, Purpose: "select",
					},
					{
						Column: Column{Database: "tracker", Table: "events", Name: "date"}, Purpose: "where",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "attributes_namespace"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'bx'",
					},
				},
			},
		},
		{
			name: "difficult case 2",
			sql: `
SELECT events
     , events.attributes_namespace as events
     , events
     , events.uuid
     , xxx + yyy
     , xxx.uuid
     , properties_items.final_price
     , events.properties_items.final_price
     , xxx.properties_items.final_price
     , n.number
FROM tracker.events as xxx
left join (select * from system.numbers limit 100) as n on n.number=toUInt64(xxx)
WHERE date=today() and (attributes_namespace='bx' as xxx)
order by 1 as yyy
limit 100`,
			db:     "db",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "events", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "events", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "events", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "uuid", Columns: []Column{{"tracker", "events", "uuid"}}},
					{Name: "xxx+yyy", Columns: []Column{{"tracker", "events", "attributes_namespace"}}},
					{Name: "uuid", Columns: []Column{{"tracker", "events", "uuid"}}},
					{Name: "properties_items.final_price", Columns: []Column{{"tracker", "events", "properties_items.final_price"}}},
					{Name: "properties_items.final_price", Columns: []Column{{"tracker", "events", "properties_items.final_price"}}},
					{Name: "properties_items.final_price", Columns: []Column{{"tracker", "events", "properties_items.final_price"}}},
					{Name: "number", Columns: []Column{{"system", "numbers", "number"}}},
				},
				ColumnUsages: []ColumnUsage{
					{Column: Column{Database: "tracker", Table: "events", Name: "attributes_namespace"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "uuid"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.final_price"}, Purpose: "select"},
					{Column: Column{Database: "system", Table: "numbers", Name: "number"}, Purpose: "select"},
					{Column: Column{Database: "system", Table: "numbers", Name: "number"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "attributes_namespace"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "date"}, Purpose: "where"},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "attributes_namespace"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'bx'",
					},
				},
			},
		},
		{
			name: "difficult case 3",
			sql: `
select share from (
with (select 1000) as total
select
	intDiv(insert_timestamp-timestamp,60)*60 as diff,
    count(action_type) as y_fact, total,
    y_fact / total as share
from tracker.events
where date>=today()-1 and date <= today()
	and server_timestamp>=now()-250
	and attributes_namespace='bx'
	and attributes_platform='android'
	and action_type='page_view'
	and diff=0
group by diff
order by y_fact desc
)`,
			db:     "tracker",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "share", Columns: []Column{
						{"tracker", "events", "action_type"},
					}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column:  Column{"tracker", "events", "action_type"},
						Purpose: "select",
					},
					{Column: Column{"tracker", "events", "insert_timestamp"}, Purpose: "select"},
					{Column: Column{"tracker", "events", "timestamp"}, Purpose: "select"},
					{Column: Column{"tracker", "events", "date"}, Purpose: "where"},
					{
						Column:         Column{"tracker", "events", "action_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'page_view'",
					},
					{Column: Column{"tracker", "events", "server_timestamp"}, Purpose: "where"},
					{
						Column:         Column{"tracker", "events", "attributes_namespace"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'bx'",
					},
					{
						Column:         Column{"tracker", "events", "attributes_platform"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'android'",
					},
					{Column: Column{"tracker", "events", "insert_timestamp"}, Purpose: "where"},
					{Column: Column{"tracker", "events", "timestamp"}, Purpose: "where"},
					{Column: Column{"tracker", "events", "action_type"}, Purpose: "order by"},
					{Column: Column{"tracker", "events", "insert_timestamp"}, Purpose: "group by"},
					{Column: Column{"tracker", "events", "timestamp"}, Purpose: "group by"},
				},
			},
		},
		{
			name: "difficult case 4",
			sql: `
select clu.host_name as hostName, coalesce(val.value, 0) as value
from system.clusters as clu
left join (
    select fullHostName() as hostName, sum(rows) as value
    from cluster('shards', system, part_log)
    where database = 'raw'
      and table = 'events'
      and event_type = 'NewPart'
      and event_time between subtractMinutes(now(), 5) and now()
    group by hostName
) as val on clu.host_name = val.hostName
where clu.cluster = 'shards'
`,
			db:     "tracker",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "hostName", Columns: []Column{{"system", "clusters", "host_name"}}},
					{Name: "value", Columns: []Column{{"system", "part_log", "rows"}}},
				},
				ColumnUsages: []ColumnUsage{
					{Column: Column{"system", "clusters", "host_name"}, Purpose: "select"},
					{Column: Column{"system", "part_log", "rows"}, Purpose: "select"},
					{
						Column:         Column{"system", "part_log", "database"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'raw'",
					},
					{
						Column:         Column{"system", "part_log", "table"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'events'",
					},
					{
						Column:         Column{"system", "part_log", "event_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'NewPart'",
					},
					{Column: Column{"system", "part_log", "event_time"}, Purpose: "where"},
					{Column: Column{"system", "clusters", "host_name"}, Purpose: "from"},
					{Column: Column{"system", "clusters", "cluster"}, Purpose: "where"},
				},
			},
		},
		{
			name: "difficult case 5",
			sql: `
SELECT  pages.date, pages.attributes_platform, pages.page_current,  pages.category_1, pages.category_2, pages.category_3, uniq(pages.user_session_id) as a2C, sum(cartContent.GMV) as GMV
FROM (
    SELECT date, user_session_id, page_current, object_sku, attributes_platform
        , dictGetString('navigation_category', 'navigation_category_name_1', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_1
        , dictGetString('navigation_category', 'navigation_category_name_2', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_2
        , dictGetString('navigation_category', 'navigation_category_name_3', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_3
    FROM tracker.events
        WHERE date = today()-122
            AND attributes_platform = 'site'
            AND attributes_namespace = 'bx'
            AND object_type = 'product'
            AND action_type = 'to_cart'
            AND dictGetUInt64('navigation_category', 'navigation_category_id_1', dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku))) IN (11000,33332)
			AND dictGetUInt64('navigation_category', 'navigation_category_id_2', dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku))) != 11650
) as pages
INNER JOIN (
    SELECT user_session_id, sku, qty*price as GMV
		 , dictGetString('navigation_category', 'navigation_category_name_1', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku)))) as category_1
    FROM (
        SELECT user_session_id, argMax(properties_items.sku, timestamp) as skuList, argMax(properties_items.quantity,timestamp) as qtyList, argMax(properties_items.final_price,timestamp) as priceList
        FROM tracker.events
        WHERE date = today()-122
          AND attributes_platform = 'site'
          AND attributes_namespace = 'bx'
          AND object_type = 'cart'
          AND action_type = 'cart_view'
        GROUP BY user_session_id
        HAVING skuList != []
    ) as maxTime
    array join skuList as sku, qtyList as qty, priceList as price
    WHERE dictGetUInt64('navigation_category', 'navigation_category_id_1', dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku))) IN (11000,33332)
    AND dictGetUInt64('navigation_category', 'navigation_category_id_2', dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku))) != 11650
) as cartContent
ON (pages.object_sku = cartContent.sku AND pages.user_session_id = cartContent.user_session_id)
GROUP BY pages.date, pages.page_current, pages.attributes_platform, pages.category_1, pages.category_2, pages.category_3
`,
			db:     "tracker",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "date", Columns: []Column{{Database: "tracker", Table: "events", Name: "date"}}},
					{Name: "attributes_platform", Columns: []Column{{Database: "tracker", Table: "events", Name: "attributes_platform"}}},
					{Name: "page_current", Columns: []Column{{Database: "tracker", Table: "events", Name: "page_current"}}},
					{Name: "category_1", Columns: []Column{{Database: "tracker", Table: "events", Name: "object_sku"}}},
					{Name: "category_2", Columns: []Column{{Database: "tracker", Table: "events", Name: "object_sku"}}},
					{Name: "category_3", Columns: []Column{{Database: "tracker", Table: "events", Name: "object_sku"}}},
					{Name: "a2C", Columns: []Column{{Database: "tracker", Table: "events", Name: "user_session_id"}}},
					{Name: "GMV", Columns: []Column{{Database: "tracker", Table: "events", Name: "properties_items.quantity"}, {Database: "tracker", Table: "events", Name: "timestamp"}, {Database: "tracker", Table: "events", Name: "properties_items.final_price"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "action_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'to_cart'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "action_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'cart_view'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "attributes_namespace"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'bx'",
					},
					{Column: Column{Database: "tracker", Table: "events", Name: "attributes_platform"}, Purpose: "group by"},
					{Column: Column{Database: "tracker", Table: "events", Name: "attributes_platform"}, Purpose: "select"},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "attributes_platform"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'site'",
					},
					{Column: Column{Database: "tracker", Table: "events", Name: "date"}, Purpose: "group by"},
					{Column: Column{Database: "tracker", Table: "events", Name: "date"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "date"}, Purpose: "where"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "group by"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "where"},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "object_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'product'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "object_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'cart'",
					},
					{Column: Column{Database: "tracker", Table: "events", Name: "page_current"}, Purpose: "group by"},
					{Column: Column{Database: "tracker", Table: "events", Name: "page_current"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.final_price"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.final_price"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.quantity"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.quantity"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.sku"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.sku"}, Purpose: "having"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.sku"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_items.sku"}, Purpose: "where"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "having"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "where"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "group by"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "select"},
				},
			},
		},
		{
			name: "difficult case 6",
			sql: `
SELECT "click_timestamp","user_session_id","click_sku","pdp_sku" FROM (select click_timestamp,
            user_session_id,
            c.object_sku click_sku,
            p.object_sku pdp_sku
    from (select user_session_id,
                    object_sku,
                    dictGet('sku', 'model_id', toUInt64(object_sku)) model_id,
                    hiveHash(user_session_id)              hash_session,
                    click_timestamp
            from (SELECT user_session_id,
                        case
                            when object_type = 'product' then object_sku
                            when object_type = 'ui' then properties_sku
                        end object_sku,
                        timestamp click_timestamp
                FROM tracker.events
                WHERE date = '2020-11-09'
                    and attributes_namespace = 'bx'
                    and action_type = 'click'
                    and ((action_widget in ('catalog.searchResultsV2', 'catalog.soldOutResultsV2')
                    and (page_current_url like '%text=%' or page_current_url LIKE '%redirect_query=%'))
                    or (action_widget in
                        ('rtb.advProductShelf.1', 'advProductShelf', 'rtb.advertisingCPMSearchShelf') and
                        page_current_url like '%text=%'))
                    ) os
                ) c
    left join (select object_sku,
                        dictGet('sku', 'model_id', toUInt64(object_sku)) model_id,
                        hiveHash(user_session_id)                        hash_session,
                        to_cart_timestamp
                from ( SELECT user_session_id,
                                object_sku,
                                timestamp to_cart_timestamp
                        FROM tracker.events
                        WHERE date = '2020-11-09'
                        and attributes_namespace = 'bx'
                        and action_type = 'to_cart'
                        and action_widget = 'pdp-widget'
                        and page_current = 'pdp') os
        ) p on c.hash_session = p.hash_session and c.model_id = p.model_id
    where click_timestamp < to_cart_timestamp) cl WHERE ("pdp_sku" IS NOT NULL) AND ("user_session_id" IS NOT NULL)
`,
			db:     "tracker",
			schema: realSchema,
			want: Stat{
				Export: []Expression{
					{Name: "click_timestamp", Columns: []Column{{Database: "tracker", Table: "events", Name: "timestamp"}}},
					{Name: "user_session_id", Columns: []Column{{Database: "tracker", Table: "events", Name: "user_session_id"}}},
					{Name: "click_sku", Columns: []Column{{Database: "tracker", Table: "events", Name: "object_type"}, {Database: "tracker", Table: "events", Name: "object_sku"}, {Database: "tracker", Table: "events", Name: "properties_sku"}}},
					{Name: "pdp_sku", Columns: []Column{{Database: "tracker", Table: "events", Name: "object_sku"}}},
				},
				ColumnUsages: []ColumnUsage{
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "action_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'click'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "action_type"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'to_cart'",
					},
					{
						Column:  Column{Database: "tracker", Table: "events", Name: "action_widget"},
						Purpose: "where",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "action_widget"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'pdp-widget'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "attributes_namespace"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'bx'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "date"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'2020-11-09'",
					},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_sku"}, Purpose: "where"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_type"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "object_type"}, Purpose: "select"},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "page_current"},
						Purpose:        "where",
						ComparisonOp:   "=",
						ComparisonWith: "'pdp'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "page_current_url"},
						Purpose:        "where",
						ComparisonOp:   "like",
						ComparisonWith: "'%redirect_query=%'",
					},
					{
						Column:         Column{Database: "tracker", Table: "events", Name: "page_current_url"},
						Purpose:        "where",
						ComparisonOp:   "like",
						ComparisonWith: "'%text=%'",
					},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_sku"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "properties_sku"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "timestamp"}, Purpose: "where"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "from"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "select"},
					{Column: Column{Database: "tracker", Table: "events", Name: "user_session_id"}, Purpose: "where"},
				},
			},
		},
		// проблема в парсере - колонка не может называться column
		//		{
		//			Name: "difficult case 7",
		//			sql: `
		//select bad_per_1_min,
		//       bad_per_5_min,
		//       bad_per_15_min,
		//       bad_per_1_hour,
		//       bad_per_3_hour
		//
		//       	from (
		//	      select quantileTDigestIf(0.990000)(clicks_per_1_min, clicks_per_1_min != 0)        q_per_1_min,
		//	             quantileTDigestIf(0.990000)(clicks_per_5_min, clicks_per_5_min != 0)     q_per_5_min,
		//	             quantileTDigestIf(0.990000)(clicks_per_15_min, clicks_per_15_min != 0)   q_per_15_min,
		//	             quantileTDigestIf(0.990000)(clicks_per_1_hour, clicks_per_1_hour != 0)   q_per_1_hour,
		//	             quantileTDigestIf(0.990000)(clicks_per_3_hour, clicks_per_3_hour != 0)   q_per_3_hour,
		//
		//	             groupArray(column)                                                      columns,
		//	             groupArray(clicks_per_1_min)                                            clicks_per_1_min_arr,
		//	             groupArray(clicks_per_5_min)                                            clicks_per_5_min_arr,
		//	             groupArray(clicks_per_15_min)                                           clicks_per_15_min_arr,
		//	             groupArray(clicks_per_1_hour)                                           clicks_per_1_hour_arr,
		//	             groupArray(clicks_per_3_hour)                                           clicks_per_3_hour_arr,
		//
		//	             arrayFilter((i, c) -> c > q_per_1_min, columns, clicks_per_1_min_arr)   bad_per_1_min,
		//	             arrayFilter((i, c) -> c > q_per_5_min, columns, clicks_per_5_min_arr)   bad_per_5_min,
		//	             arrayFilter((i, c) -> c > q_per_15_min, columns, clicks_per_15_min_arr) bad_per_15_min,
		//	             arrayFilter((i, c) -> c > q_per_1_hour, columns, clicks_per_1_hour_arr) bad_per_1_hour,
		//	             arrayFilter((i, c) -> c > q_per_3_hour, columns, clicks_per_3_hour_arr) bad_per_3_hour
		//	      from (
		//	            select IPv4NumToString(user_ip)                                                     column,
		//	                   countIf(server_timestamp >= subtractMinutes(now(), 1))  clicks_per_1_min,
		//	                   countIf(server_timestamp >= subtractMinutes(now(), 5))  clicks_per_5_min,
		//	                   countIf(server_timestamp >= subtractMinutes(now(), 15)) clicks_per_15_min,
		//	                   countIf(server_timestamp >= subtractHours(now(), 1))    clicks_per_1_hour,
		//	                   countIf(server_timestamp >= subtractHours(now(), 3))    clicks_per_3_hour
		//	            from tracker.events
		//	            where action_type = 'page_view'
		//	              and date >= toDate(subtractHours(now(), 3))
		//	              and server_timestamp >= subtractHours(now(), 3)
		//	              and (user_ip < IPv4StringToNum('91.223.93.0') or user_ip > IPv4StringToNum('91.223.93.255')) AND (user_ip < IPv4StringToNum('109.194.79.227') or user_ip > IPv4StringToNum('109.194.79.227')) AND (user_ip < IPv4StringToNum('185.73.192.0') or user_ip > IPv4StringToNum('185.73.195.255')) AND (user_ip < IPv4StringToNum('195.34.21.0') or user_ip > IPv4StringToNum('195.34.21.255')) AND (user_ip < IPv4StringToNum('62.105.128.170') or user_ip > IPv4StringToNum('62.105.128.170')) AND (user_ip < IPv4StringToNum('85.235.162.70') or user_ip > IPv4StringToNum('85.235.162.70')) AND (user_ip < IPv4StringToNum('213.33.200.166') or user_ip > IPv4StringToNum('213.33.200.166')) AND (user_ip < IPv4StringToNum('185.73.195.0') or user_ip > IPv4StringToNum('185.73.195.255')) AND (user_ip < IPv4StringToNum('10.0.0.0') or user_ip > IPv4StringToNum('10.255.255.255')) -- internal networks filter
		//	            group by column
		//	             )
		//	       )
		//`,
		//			db:     "tracker",
		//			schema: realSchema,
		//			want: Stat{
		//				export: []Expression{
		//					//{Name: "click_timestamp", Columns: []Column{{Database: "tracker", table: "events", Name: "timestamp"}}},
		//				},
		//				columnUsages: []ColumnUsage{
		//					//{Column: Column{Database: "tracker", table: "events", Name: "action_type"}, Purpose: "where"},
		//				},
		//			},
		//		},
	}

	//debug = true // set true to see debug logs

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analyser := NewColumnUsageAnalyser(tt.schema)
			res, err := analyser.ParseQuery(tt.db, tt.sql)
			if tt.error {
				assert.Error(t, err)
				return
			}
			if err != nil {
				t.Log(err)
				t.Fail()
				return
			}
			assert.Equal(t, tt.want.Export, res.Export)
			assert.Equal(t, usagesIntoMap(tt.want.ColumnUsages), usagesIntoMap(res.ColumnUsages))
		})
	}
}

func usagesIntoMap(usages []ColumnUsage) map[ColumnUsage]bool {
	mp := make(map[ColumnUsage]bool)
	for _, usage := range usages {
		mp[usage] = true
	}
	return mp
}

func BenchmarkParser(b *testing.B) {
	analyser := NewColumnUsageAnalyser(realSchema)
	sql := `
SELECT  pages.date, pages.attributes_platform, pages.page_current,  pages.category_1, pages.category_2, pages.category_3, uniq(pages.user_session_id) as a2C, sum(cartContent.GMV) as GMV
FROM (
    SELECT date, user_session_id, page_current, object_sku, attributes_platform
        , dictGetString('navigation_category', 'navigation_category_name_1', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_1
        , dictGetString('navigation_category', 'navigation_category_name_2', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_2
        , dictGetString('navigation_category', 'navigation_category_name_3', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku)))) as category_3
    FROM tracker.events
        WHERE date = today()-122
            AND attributes_platform = 'site'
            AND attributes_namespace = 'bx'
            AND object_type = 'product'
            AND action_type = 'to_cart'
            AND dictGetUInt64('navigation_category', 'navigation_category_id_1', dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku))) IN (11000,33332)
			AND dictGetUInt64('navigation_category', 'navigation_category_id_2', dictGetUInt64('sku', 'navigation_category_id', toUInt64(object_sku))) != 11650
) as pages
INNER JOIN (
    SELECT user_session_id, sku, qty*price as GMV
		 , dictGetString('navigation_category', 'navigation_category_name_1', toUInt64(dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku)))) as category_1
    FROM (
        SELECT user_session_id, argMax(properties_items.sku, timestamp) as skuList, argMax(properties_items.quantity,timestamp) as qtyList, argMax(properties_items.final_price,timestamp) as priceList
        FROM tracker.events
        WHERE date = today()-122
          AND attributes_platform = 'site'
          AND attributes_namespace = 'bx'
          AND object_type = 'cart'
          AND action_type = 'cart_view'
        GROUP BY user_session_id
        HAVING skuList != []
    ) as maxTime
    array join skuList as sku, qtyList as qty, priceList as price
    WHERE dictGetUInt64('navigation_category', 'navigation_category_id_1', dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku))) IN (11000,33332)
    AND dictGetUInt64('navigation_category', 'navigation_category_id_2', dictGetUInt64('sku', 'navigation_category_id', toUInt64(sku))) != 11650
) as cartContent
ON (pages.object_sku = cartContent.sku AND pages.user_session_id = cartContent.user_session_id)
GROUP BY pages.date, pages.page_current, pages.attributes_platform, pages.category_1, pages.category_2, pages.category_3`

	for n := 0; n < b.N; n++ {
		_, err := analyser.ParseQuery("", sql)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}
