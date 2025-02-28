from prometheus_client.parser import text_string_to_metric_families
import requests

def parse_metrics(metrics):
    fs_stat_metrics = {'avail_bytes': None, 'size_bytes': None}
    for family in text_string_to_metric_families(metrics):
        avail_bytes = [sample.value for sample in family.samples if sample.name == 'node_filesystem_avail_bytes' and
                sample.labels.get('device') == '/dev/sdb' and
                sample.labels.get('fstype') == 'ext4']
        size_bytes = [sample.value for sample in family.samples if sample.name == 'node_filesystem_size_bytes' and
                sample.labels.get('device') == '/dev/sdb' and
                sample.labels.get('fstype') == 'ext4']
        if len(avail_bytes) > 0:
            fs_stat_metrics['avail_bytes'] = round(avail_bytes[0] / 1024 ** 3)
        elif len(size_bytes) > 0:
            fs_stat_metrics['size_bytes'] = round(size_bytes[0] / 1024 ** 3)
    return fs_stat_metrics
    

req = requests.get('http://someURL:9100/metrics')
fs_stats = parse_metrics(req.text)
print(fs_stats)
