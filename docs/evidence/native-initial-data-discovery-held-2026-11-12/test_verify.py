"""Six semantic negative controls against owned copies; original evidence unchanged."""
import copy
import unittest
import verify

class TamperTests(unittest.TestCase):
    def setUp(self):
        self.audit, self.rpc = verify.payloads()
    def phase(self, name):
        return next(x for x in self.audit if x['phase'] == name)
    def reject(self):
        with self.assertRaises(verify.Invalid):
            verify.semantics(self.audit, self.rpc)
    def test_foreign_source(self):
        self.phase('begin')['source_sha'] = '0'*40
        self.reject()
    def test_not_all_actual_services_held(self):
        self.phase('held_metadata_qq')['all_actual_service_latches_held'] = 2
        self.reject()
    def test_reused_metadata_context(self):
        self.phase('held_metadata_qq_after_refresh')['context'] = self.phase('held_metadata_qq')['context']
        self.reject()
    def replace_frame(self, record, value):
        import json
        data = json.dumps(value).encode()
        record['raw_frame'] = list(len(data).to_bytes(4, 'big') + data)
    def test_wrong_response_identity(self):
        cut = self.phase('held_public_refresh')
        reply = next(x for x in self.rpc[cut['rpc_record_start']:cut['rpc_record_end']]
                     if x['phase'] == 'response_frame_before_decode')
        value = verify.envelope(reply)
        value['source_node_id'] = 'foreign'
        self.replace_frame(reply, value)
        self.reject()
    def test_fallback_refusal_replaced_by_success(self):
        cut = self.phase('held_public_refresh')
        reply = [x for x in self.rpc[cut['rpc_record_start']:cut['rpc_record_end']]
                 if x['phase'] == 'response_frame_before_decode'][1]
        value = verify.envelope(reply)
        value['response']['Err']['message'] = 'invented successful fallback'
        self.replace_frame(reply, value)
        self.reject()
    def test_present_null_replaced_by_absence(self):
        reply = next(x for x in self.rpc if x['phase'] == 'response_frame_before_decode'
                     and x['operation'] == 'native_event_get' and x['destination'] == 'n1')
        value = verify.envelope(reply)
        value['response']['Ok']['NativeEventGet']['result']['value'] = {'presence': 'absent'}
        self.replace_frame(reply, value)
        self.reject()

if __name__ == '__main__':
    unittest.main()
