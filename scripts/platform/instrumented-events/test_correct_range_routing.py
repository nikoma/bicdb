import unittest
import correct_range_routing as helper


class RangeRoutingTests(unittest.TestCase):
    def catalog(self):
        return {'ranges':{'18':{'current_epoch':14,'last_index':59,'resolved_through':59,'compacted_through':0}}}

    def test_nonzero_token_map_key_is_distinct_from_range_id(self):
        topology={'ranges':{'9223372036854775808':{'id':18,'epoch':17,'leader':'n0','replicas':[]}}}
        rows=helper.correct_rows(self.catalog(),topology)
        self.assertEqual(18,rows[0]['range_id'])
        self.assertEqual('9223372036854775808',rows[0]['token_start_map_key'])
        self.assertEqual(17,rows[0]['independently_copied_route_descriptor']['epoch'])
        self.assertEqual(14,rows[0]['retained_command_epoch'])
        self.assertTrue(rows[0]['routing_epoch_equality_is_not_a_recovery_health_gate'])

    def test_duplicate_descriptor_id_is_rejected(self):
        topology={'ranges':{'0':{'id':18},'9223372036854775808':{'id':18}}}
        with self.assertRaisesRegex(ValueError,'duplicate range'):helper.correct_rows(self.catalog(),topology)

    def test_missing_descriptor_is_rejected_instead_of_null_projection(self):
        topology={'ranges':{'0':{'id':19}}}
        with self.assertRaisesRegex(ValueError,'missing range'):helper.correct_rows(self.catalog(),topology)


if __name__=='__main__':unittest.main()
