// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arrange and reduce operators that can start from restored checkpoint state.
//!
//! These are copies of differential-dataflow 0.26's `arrange_core` and `reduce_with_tactic`.
//! Every change from upstream is marked `CHECKPOINT:`. Restoring means three things:
//!
//! * the trace is seeded with one span covering `[minimum, F)` before the operator first runs,
//! * that span is never sent downstream, so consumers see only updates at `F` and later, and
//! * the operator's internal frontiers start at `F`, so reduce reads the seeded traces through
//!   `F` when it retires its first interval.

use std::any::Any;

use differential_dataflow::batcher::Batcher;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{self, Description, ExertionLogic, SpanOf, Trace, TraceReader};
use timely::Container;
use timely::PartialOrder;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::progress::{Antichain, Stamp, Timestamp};

use crate::checkpoint::{self, Site};

/// Downcasts a restored span to the trace's span type.
fn restored_span<Tr: TraceReader + 'static>(restored: Box<dyn Any>) -> SpanOf<Tr>
where
    SpanOf<Tr>: 'static,
{
    *restored
        .downcast::<SpanOf<Tr>>()
        .expect("checkpoint codec matches the trace type")
}

/// `arrange_core`, seeded with `site.restored` and followed by a checkpoint writer.
pub(crate) fn arrange_restorable<'scope, P, C, Ba, Tr>(
    stream: Stream<'scope, Tr::Time, C>,
    pact: P,
    name: &str,
    batcher: impl FnOnce(Option<Logger>, usize) -> Ba,
    mut site: Site,
) -> Arranged<'scope, TraceAgent<Tr>>
where
    C: Container + Clone + 'static,
    P: ParallelizationContract<Tr::Time, C>,
    Ba: Batcher<C, Time = Tr::Time, Output: Into<Tr::Batch>> + 'static,
    Tr: Trace + 'static,
    Tr::Time: Timestamp + Lattice,
{
    let mut reader: Option<TraceAgent<Tr>> = None;
    let reader_ref = &mut reader;
    let scope = stream.scope();
    // CHECKPOINT: take the restored span before building the operator.
    let restored = site.restored.take().map(restored_span::<Tr>);
    // A seeded generation's writer must write the restored span too.
    let seed = restored.clone();

    let stream = stream.unary_frontier(pact, name, move |_capability, info| {
        let logger = scope
            .worker()
            .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                "differential/arrange",
            )
            .map(Into::into);
        let mut batcher = batcher(logger.clone(), info.global_id);
        let mut capabilities = Antichain::<Capability<Tr::Time>>::new();
        let activator = Some(scope.activator_for(std::rc::Rc::clone(&info.address)));
        let mut empty_trace = Tr::new(info.clone(), logger.clone(), activator);
        if let Some(exert_logic) = scope
            .worker()
            .config()
            .get::<ExertionLogic>("differential/default_exert_logic")
            .cloned()
        {
            empty_trace.set_exert_logic(exert_logic);
        }
        let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);
        *reader_ref = Some(reader_local);

        let mut prev_frontier = Antichain::from_elem(<Tr::Time as Timestamp>::minimum());

        // CHECKPOINT: seed the trace with the restored span, without sending it downstream, and
        // start from its upper.
        if let Some(span) = restored {
            prev_frontier = span.upper().clone();
            writer.insert(span, Stamp::new());
        }
        let restored_upper = prev_frontier.clone();

        move |(input, frontier), output| {
            input.for_each(|cap, data| {
                for capability in cap.retain_stamp(0).iter() {
                    // CHECKPOINT: a capability before the restored upper would become the hint of
                    // a span after it, and an importer of this trace, already at the restored
                    // upper, panics creating a capability for it. Input data must start at the
                    // restored upper, so the capability can be delayed to it.
                    let mut time = capability.time().clone();
                    time.advance_by(restored_upper.borrow());
                    capabilities.insert(capability.delayed(&time));
                }
                batcher.insert(data);
            });

            // CHECKPOINT: the input frontier starts below the restored upper, and no data can
            // arrive there. Upstream asserts the frontier never regresses below `prev_frontier`.
            if !PartialOrder::less_equal(&prev_frontier.borrow(), &frontier.frontier()) {
                return;
            }

            if prev_frontier.borrow() != frontier.frontier() {
                if capabilities
                    .elements()
                    .iter()
                    .any(|c| !frontier.less_equal(c.time()))
                {
                    let retired = capabilities
                        .elements()
                        .iter()
                        .filter(|c| !frontier.less_equal(c.time()))
                        .cloned()
                        .collect::<CapabilitySet<_>>();
                    let description = Description::new(
                        prev_frontier.clone(),
                        frontier.frontier().to_owned(),
                        Antichain::from_elem(<Tr::Time as Timestamp>::minimum()),
                    );
                    let (extracted, retained) = batcher.extract(frontier.frontier());
                    let batch = trace::Span::new(description, extracted.map(Into::into));
                    let stamp = retired
                        .iter()
                        .map(|c| c.time().clone())
                        .collect::<Stamp<_>>();
                    writer.insert(batch.clone(), stamp);
                    output.session(&retired).give(batch);

                    let mut new_capabilities = Antichain::new();
                    for time in retained.iter() {
                        if let Some(capability) = capabilities
                            .elements()
                            .iter()
                            .find(|c| c.time().less_equal(time))
                        {
                            new_capabilities.insert(capability.delayed(time));
                        } else {
                            panic!("failed to find capability");
                        }
                    }
                    capabilities = new_capabilities;
                } else {
                    writer.seal(frontier.frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(frontier.frontier().iter().cloned());
            }

            writer.exert();
        }
    });

    // CHECKPOINT: follow the batch stream with a checkpoint writer.
    let trace = reader.unwrap();
    checkpoint::attach_writer(stream.clone(), trace.clone(), site, seed);

    Arranged { stream, trace }
}

/// `reduce_with_tactic`, with its output trace seeded from `site.restored`, its frontiers starting
/// at the restored upper, and followed by a checkpoint writer.
pub(crate) fn reduce_restorable<'scope, Tr1, Tr2, T>(
    trace: Arranged<'scope, Tr1>,
    name: &str,
    mut tactic: T,
    mut site: Site,
) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader + 'static,
    Tr2: Trace<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Time, Tr1::Batch, Tr2::Batch> + 'static,
{
    let mut result_trace = None;
    // CHECKPOINT: take the restored span before building the operator.
    let restored = site.restored.take().map(restored_span::<Tr2>);
    // A seeded generation's writer must write the restored span too.
    let seed = restored.clone();

    let stream = {
        let mut source_trace = trace.trace;
        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace
            .stream
            .unary_frontier(Pipeline, name, move |_capability, operator_info| {
                let logger = scope
                    .worker()
                    .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                        "differential/arrange",
                    )
                    .map(Into::into);
                let activator =
                    Some(scope.activator_for(std::rc::Rc::clone(&operator_info.address)));
                let mut empty = Tr2::new(operator_info.clone(), logger.clone(), activator);
                if let Some(exert_logic) = scope
                    .worker()
                    .config()
                    .get::<ExertionLogic>("differential/default_exert_logic")
                    .cloned()
                {
                    empty.set_exert_logic(exert_logic);
                }
                let (mut output_reader, mut output_writer) =
                    TraceAgent::new(empty, operator_info, logger);
                *result_trace = Some(output_reader.clone());

                let mut capabilities = CapabilitySet::<Tr1::Time>::default();

                let minimum = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
                // CHECKPOINT: seed the output trace, and start every frontier at the restored
                // upper. The restored input arrangement ends at the same upper, so
                // `batches_through` finds a batch boundary there on both traces.
                let start = match restored {
                    Some(span) => {
                        let upper = span.upper().clone();
                        output_writer.insert(span, Stamp::new());
                        upper
                    }
                    None => minimum,
                };
                let mut upper_limit = start.clone();
                let mut lower_limit = start.clone();
                let mut source_upper = start.clone();
                let mut source_lower = start;

                move |(input, frontier), output| {
                    let mut batch_storage = Vec::new();

                    lower_limit.clear();
                    lower_limit.extend(upper_limit.borrow().iter().cloned());
                    source_lower.clone_from(&source_upper);

                    input.for_each(|capability, batches| {
                        for capability in capability.retain_stamp(0).iter() {
                            capabilities.insert(capability.clone());
                        }
                        for batch in batches.drain(..) {
                            source_upper.clone_from(batch.upper());
                            batch_storage.push(batch);
                        }
                    });

                    source_trace.advance_upper(&mut source_upper);
                    let mut joined = Antichain::new();
                    differential_dataflow::lattice::antichain_join_into(
                        &source_upper.borrow()[..],
                        &frontier.frontier()[..],
                        &mut joined,
                    );
                    upper_limit = joined;

                    if upper_limit != lower_limit {
                        let source_batches = source_trace
                            .batches_through(source_lower.borrow())
                            .expect("failed to acquire source batches");
                        let output_batches = output_reader
                            .batches_through(lower_limit.borrow())
                            .expect("failed to acquire output batches");
                        let held: Antichain<Tr1::Time> =
                            capabilities.iter().map(|c| c.time().clone()).collect();
                        let (produced, new_frontier) = tactic.retire(
                            source_batches,
                            output_batches,
                            batch_storage.into_iter().filter_map(|b| b.inner).collect(),
                            &lower_limit,
                            &upper_limit,
                            &held,
                        );

                        if let Some(batch) = produced {
                            let retiring = capabilities
                                .iter()
                                .filter(|c| !upper_limit.less_equal(c.time()))
                                .cloned()
                                .collect::<CapabilitySet<_>>();
                            output.session(&retiring).give(batch.clone());
                            let stamp = retiring
                                .iter()
                                .map(|c| c.time().clone())
                                .collect::<Stamp<_>>();
                            output_writer.insert(batch, stamp);
                        }

                        capabilities.downgrade(new_frontier);
                        output_writer.seal(upper_limit.clone());

                        source_trace.set_logical_compaction(upper_limit.borrow());
                        output_reader.set_logical_compaction(upper_limit.borrow());
                        source_trace.set_physical_compaction(source_upper.borrow());
                        output_reader.set_physical_compaction(upper_limit.borrow());
                    }

                    output_writer.exert();
                }
            })
    };

    // CHECKPOINT: follow the output stream with a checkpoint writer.
    let trace = result_trace.unwrap();
    checkpoint::attach_writer(stream.clone(), trace.clone(), site, seed);

    Arranged { stream, trace }
}
