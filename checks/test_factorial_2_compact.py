"""Compact diagnostics retain exact counts, full primary events and strict delivery."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'src/eclipse_hivemind/nodes')]
from eclipse_hivemind.nodes.factorial2_compact.compact import CounterWindow, valid_summary, aggregate_kind
from eclipse_hivemind.nodes.factorial2_compact.trace import Trace
from eclipse_hivemind.nodes.factorial2_compact import trace_full


class CompactTests(unittest.TestCase):
    def test_hundred_thousand_repeated_filter_payloads_have_bounded_counter_memory(self):
        w=CounterWindow('support-0')
        for i in range(100_000):
            self.assertTrue(w.add('response_filtered',dict(retained=['a'],removed=['b','c'],requester_peer_id=str(i)),i))
        self.assertEqual(len(w.counts),1)
        self.assertEqual(len(w.details),3)
        d=w.pop()
        self.assertTrue(valid_summary('support-0',d))
        self.assertEqual(d['event_counts'],{'response_filtered':100_000})
        self.assertEqual(d['diagnostic_totals']['filter_removed_announcements'],200_000)
        self.assertLess(len(json.dumps(d)),500)
        self.assertIsNone(w.pop())

    def test_all_primary_and_error_events_and_unknown_future_kinds_are_preserved(self):
        w=CounterWindow('victim-0')
        for kind in ('candidate_snapshot','group_formed','allreduce_completed','averaging_contribution',
                     'storage_snapshot','storage_control','node_error','observation_error','lookup_failed',
                     'telemetry_warning','new_unknown_kind'):
            self.assertFalse(w.add(kind,{},1),kind)
        self.assertTrue(aggregate_kind('support-0','candidate_snapshot'))
        self.assertFalse(aggregate_kind('victim-0','candidate_snapshot'))

    def test_counter_validation_refuses_forged_primary_counts_and_malformed_totals(self):
        w=CounterWindow('victim-0');w.add('dht_response',{},1);d=w.pop()
        for update in (dict(event_counts={'group_formed':1}),dict(compacted_events=2),dict(summary_index=-1),
                       dict(event_counts={'dht_response':True}),dict(event_counts={'dht_response':-1}),
                       dict(event_counts={'candidate_snapshot':1})):
            self.assertFalse(valid_summary('victim-0',{**d,**update}))

    def trace(self,node='victim-0'):
        return Trace(dict(experiment_id='offline',node_id=node,node_type='victim',node_index=0),'http://unused')

    def test_real_spool_forwarder_flushes_final_window_and_audits_exact_counts(self):
        delivered=[]
        def send(**kwargs):
            delivered.extend(kwargs['observations'])
            return dict(accepted=len(kwargs['observations']),duplicates=0)
        t=self.trace()
        with patch.object(trace_full.client,'send_eclipse_observations',side_effect=send),contextlib.redirect_stdout(io.StringIO()):
            for i in range(10_000):t.emit('response_filtered',removed=['same-peer'],retained=[])
            t.emit('candidate_snapshot',query_id='keep-me',candidates=[{'peer_id':'x'}])
            t.emit('observation_error',source='test',error='retained')
            t.close()
        self.assertTrue(t.completed)
        self.assertFalse(t.directory.exists())
        counts=[x for x in delivered if x['kind']=='telemetry_event_counts']
        self.assertEqual(sum(x['event_counts'].get('response_filtered',0) for x in counts),10_000)
        self.assertEqual(len([x for x in delivered if x['kind']=='candidate_snapshot']),1)
        self.assertEqual(len([x for x in delivered if x['kind']=='observation_error']),1)
        audit=delivered[-1]
        self.assertEqual(audit['transport_version'],'process_spool_compact_v1')
        self.assertEqual(audit['compacted_events_by_kind'],{'response_filtered':10_000})
        self.assertEqual(audit['compact_summary_records'],len(counts))
        self.assertEqual(audit['acknowledged_observations'],len(delivered)-1)

    def test_concurrent_emitters_do_not_lose_counts(self):
        t=self.trace('support-0');delivered=[]
        def send(**kw):
            delivered.extend(kw['observations']);return {'accepted':len(kw['observations'])}
        with patch.object(trace_full.client,'send_eclipse_observations',side_effect=send),contextlib.redirect_stdout(io.StringIO()):
            threads=[threading.Thread(target=lambda:[t.emit('dht_response',records=[]) for _ in range(5000)]) for _ in range(4)]
            for th in threads:th.start()
            for th in threads:th.join()
            t.close()
        self.assertEqual(delivered[-1]['compacted_events_by_kind'],{'dht_response':20_000})

    def test_aggregated_event_after_terminal_marker_still_fails(self):
        t=self.trace()
        with patch.object(trace_full.client,'send_eclipse_observations',side_effect=lambda **kw:{'accepted':len(kw['observations'])}),contextlib.redirect_stdout(io.StringIO()):
            t.emit('dht_response',records=[])
            t.producer_finished()
            with self.assertRaisesRegex(RuntimeError,'after terminal'):
                t.emit('dht_response',records=[])
            t.close()
        self.assertTrue(t.completed)

    def test_partial_ack_is_still_invalid_and_spools_remain(self):
        t=self.trace()
        with patch.object(trace_full.client,'send_eclipse_observations',return_value={'accepted':0}),contextlib.redirect_stdout(io.StringIO()):
            t.emit('response_filtered',removed=[])
            with self.assertRaisesRegex(RuntimeError,'incomplete telemetry'):t.close()
        self.assertFalse(t.completed)
        self.assertTrue(t.directory.exists())
        import shutil
        shutil.rmtree(t.directory)


if __name__=='__main__':unittest.main()

class CounterEvidenceTests(unittest.TestCase):
    def records(self):
        from checks.test_sc2_workflow import cell_records
        records=cell_records('c0-f1-p0','fixture',0)
        for r in records:
            if r['event']['event_type']=='node_started':
                r['event']['data']['runtime_metadata']['bucket_size']=5
        # Replace a non-primary transport probe with a counted diagnostic.
        target=next(r for r in records if r['node']['node_id']=='support-0' and r['event']['data'].get('kind')=='gradient_transform_summary')
        position=records.index(target)
        data=dict(trace_schema_version=2,kind='telemetry_event_counts',process_id=1,trace_sequence=1,
                  compact_schema='rpc-counters-v1',summary_index=0,event_counts={'response_filtered':137},compacted_events=137)
        import copy
        counted=copy.deepcopy(target);counted['event']['data']=data;records.insert(position,counted)
        for r in records[position+1:]:
            if r['node']['node_id']=='support-0':
                d=r['event']['data']
                if d.get('kind')=='telemetry_complete':
                    d['acknowledged_observations']+=1;d['producer_record_counts']['1']+=1
                    d['compact_summary_records']=1;d['compacted_events_by_kind']={'response_filtered':137}
                elif 'trace_sequence' in d:d['trace_sequence']+=1
        return records

    def analyse(self,records):
        from eclipse_hivemind.factorial2.evidence import analyse
        return analyse((json.dumps(r) for r in records),dict(cell='c0-f1-p0',seed=17,run_id='fixture'))

    def test_exact_filter_count_and_terminal_audit_are_accepted(self):
        result=self.analyse(self.records())
        self.assertTrue(result['technical_valid'],result['validity_reasons'])
        self.assertEqual(result['filter_events'],137)

    def test_missing_changed_and_forged_counter_audits_fail(self):
        for change in ('count','index','total','primary','audit'):
            records=self.records()
            summary=next(r['event']['data'] for r in records if r['event']['data'].get('kind')=='telemetry_event_counts')
            audit=next(r['event']['data'] for r in records if r['node']['node_id']=='support-0' and r['event']['data'].get('kind')=='telemetry_complete')
            if change=='count':summary['event_counts']['response_filtered']=138
            if change=='index':summary['summary_index']=1
            if change=='total':summary['compacted_events']=0
            if change=='primary':summary['event_counts']={'group_formed':137}
            if change=='audit':audit['compact_summary_records']=0
            result=self.analyse(records)
            self.assertFalse(result['technical_valid'],change)
