/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

use prometheus::core::Collector;
use prometheus::proto::{MetricFamily};
use obkv::client::table_client::{OBKV_CLIENT_OPERATION_HISTOGRAM_VEC};
use obkv::{OBKV_PROXY_HISTOGRAM_NUM_VEC, OBKV_RPC_HISTOGRAM_VEC, OBKV_RPC_HISTOGRAM_NUM_VEC};


pub fn print_matric_family_detail(metric_family: MetricFamily) {
    let metric_name = metric_family.get_name();
    let metric_help = metric_family.get_help();
    for metric in metric_family.get_metric() {
        let metric_label = metric.get_label();
        let metric_histogram = metric.get_histogram();
        let metric_sum = metric_histogram.get_sample_sum();
        let metric_count = metric_histogram.get_sample_count();
        let metric_average = metric_sum / metric_count as f64;
        println!("Family Name: {metric_name:?}\nFamily Help: {metric_help:?}\nType: {metric_label:?}\nSum: {metric_sum} s\nCount: {metric_count} op\nAverage: {metric_average} s/op");
        // print bucket
        let metric_buckets = metric_histogram.get_bucket();
        for metric_bucket in metric_buckets {
            let metric_bucket_upper_bound = metric_bucket.get_upper_bound();
            let metric_bucket_count = metric_bucket.get_cumulative_count();
            let percentage = metric_bucket_count as f64 / metric_count as f64 * 100.0;
            println!("Bucket: {metric_bucket_upper_bound} s\n\tCount: {metric_bucket_count} ops\n\tPercentage: {percentage} %");
        }
        println!();
    }
}

pub fn print_matric_family_num_detail(metric_family: MetricFamily) {
    let metric_name = metric_family.get_name();
    let metric_help = metric_family.get_help();
    for metric in metric_family.get_metric() {
        let metric_label = metric.get_label();
        let metric_histogram = metric.get_histogram();
        let metric_sum = metric_histogram.get_sample_sum();
        let metric_count = metric_histogram.get_sample_count();
        let metric_average = metric_sum / metric_count as f64;
        println!("Family Name: {metric_name:?}\nFamily Help: {metric_help:?}\nType: {metric_label:?}\nSum: {metric_sum}\nCount: {metric_count}\nAverage: {metric_average}");
        // print bucket
        let metric_buckets = metric_histogram.get_bucket();
        for metric_bucket in metric_buckets {
            let metric_bucket_upper_bound = metric_bucket.get_upper_bound();
            let metric_bucket_count = metric_bucket.get_cumulative_count();
            let percentage = metric_bucket_count as f64 / metric_count as f64 * 100.0;
            println!("Bucket: {metric_bucket_upper_bound}\n\tCount: {metric_bucket_count}\n\tPercentage: {percentage} %");
        }
        println!();
    }
}

pub fn print_matrics_detail(metric_families: Vec<MetricFamily>) {
    for metric_family in metric_families {
        print_matric_family_detail(metric_family);
    }
}

pub fn print_matrics_num_detail(metric_families: Vec<MetricFamily>) {
    for metric_family in metric_families {
        print_matric_family_num_detail(metric_family);
    }
}

pub fn print_client_matrics() {
    let metric_families = OBKV_CLIENT_OPERATION_HISTOGRAM_VEC.collect();
    print_matrics_detail(metric_families);
}

pub fn print_proxy_matrics() {
    let metric_families = OBKV_PROXY_HISTOGRAM_NUM_VEC.collect();
    print_matrics_detail(metric_families);
}

pub fn print_rpc_matrics() {
    let metric_families = OBKV_RPC_HISTOGRAM_VEC.collect();
    print_matrics_detail(metric_families);
}

pub fn print_rpc_num_matrics() {
    let metric_families = OBKV_RPC_HISTOGRAM_NUM_VEC.collect();
    print_matrics_num_detail(metric_families);
}