// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use columnation::Columnation;
use differential_dataflow::Data;
use differential_dataflow::difference::Abelian;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::cursor::reduce::CursorTactic;
use differential_dataflow::trace::cursor::{BatchCursor, BatchDiff, BatchVal, BatchValOwn};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Builder, Cursor, Navigable, Trace, TraceReader};
use mz_timely_util::columnation::ColumnationStack;
use timely::Container;
use timely::container::PushInto;

use crate::extensions::arrange::ArrangementSize;

pub trait ClearContainer {
    fn clear(&mut self);
}

impl<T> ClearContainer for Vec<T> {
    fn clear(&mut self) {
        Vec::clear(self)
    }
}

impl<D, T, R> ClearContainer for ColumnationStack<(D, T, R)>
where
    D: Columnation + Clone + 'static,
    T: Columnation + Clone + 'static,
    R: Columnation + Clone + 'static,
{
    fn clear(&mut self) {
        ColumnationStack::clear(self)
    }
}

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<'scope, T1: TraceReader> {
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2, KC>(
        self,
        name: &str,
        logic: L,
    ) -> Arranged<'scope, TraceAgent<T2>>
    where
        // `KC` is a distinct parameter, not `T1::KeyContainer`: pinning it from the concrete input
        // lets the compiler normalize the higher-ranked key equality on both cursors to
        // `KC::ReadItem`, which a projection-to-projection bound would not.
        T1: TraceReader<Batch: Navigable>,
        KC: BatchContainer,
        BatchCursor<T1>: Cursor<Time = T1::Time, KeyContainer = KC>,
        for<'a> BatchCursor<T1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        T2: Trace<Batch: Navigable, Time = T1::Time> + 'static,
        BatchCursor<T2>: Cursor<Time = T2::Time, KeyContainer = KC>,
        for<'a> BatchCursor<T2>:
            Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = T2::Time, Diff: Abelian>,
        Bu: Builder<Time = T1::Time, Output = T2::Batch> + 'static,
        Bu::Input: Container
            + Default
            + ClearContainer
            + PushInto<((KC::Owned, BatchValOwn<T2>), T2::Time, BatchDiff<T2>)>,
        L: FnMut(
                KC::ReadItem<'_>,
                &[(BatchVal<'_, T1>, BatchDiff<T1>)],
                &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
            ) + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize;
}

impl<'scope, T1> MzReduce<'scope, T1> for Arranged<'scope, T1>
where
    T1: TraceReader + Clone + 'static,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2, KC>(
        self,
        name: &str,
        logic: L,
    ) -> Arranged<'scope, TraceAgent<T2>>
    where
        T1: TraceReader<Batch: Navigable>,
        KC: BatchContainer,
        BatchCursor<T1>: Cursor<Time = T1::Time, KeyContainer = KC>,
        for<'a> BatchCursor<T1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        T2: Trace<Batch: Navigable, Time = T1::Time> + 'static,
        BatchCursor<T2>: Cursor<Time = T2::Time, KeyContainer = KC>,
        for<'a> BatchCursor<T2>:
            Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = T2::Time, Diff: Abelian>,
        Bu: Builder<Time = T1::Time, Output = T2::Batch> + 'static,
        Bu::Input: Container
            + Default
            + ClearContainer
            + PushInto<((KC::Owned, BatchValOwn<T2>), T2::Time, BatchDiff<T2>)>,
        L: FnMut(
                KC::ReadItem<'_>,
                &[(BatchVal<'_, T1>, BatchDiff<T1>)],
                &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
            ) + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize,
    {
        // The push closure clears its buffer between keys (the reduce operator cannot reset it,
        // and failing to clear leaks one key's rows into the next), then stages each value update
        // with the owned key prepended.
        let push_closure =
            |buf: &mut Bu::Input,
             key: KC::ReadItem<'_>,
             updates: &mut Vec<(BatchValOwn<T2>, T2::Time, BatchDiff<T2>)>| {
                buf.clear();
                let key_owned = KC::into_owned(key);
                for (val, time, diff) in updates.drain(..) {
                    buf.push_into(((key_owned.clone(), val), time, diff));
                }
            };

        if let Some(site) = crate::checkpoint::next_site::<T2>() {
            // `Arranged::reduce_abelian`'s adaptation of `logic`, applied here because the
            // restorable operator takes a tactic.
            let mut logic = logic;
            let tactic = CursorTactic::<T1::Batch, T2::Batch, Bu, _, _>::new(
                reduce_core_logic::<T1, T2, KC, _>(move |key, input, output, change| {
                    if !input.is_empty() {
                        logic(key, input, change);
                    }
                    change.extend(output.drain(..).map(|(x, mut d)| {
                        d.negate();
                        (x, d)
                    }));
                    differential_dataflow::consolidation::consolidate(change);
                }),
                push_closure,
            );
            return crate::extensions::restorable::reduce_restorable::<_, T2, _>(
                self, name, tactic, site,
            )
            .log_arrangement_size();
        }

        // Allow access to `reduce_abelian` since we're within Mz's wrapper and force arrangement
        // size logging.
        #[allow(clippy::disallowed_methods)]
        Arranged::<_>::reduce_abelian::<_, Bu, T2, KC, _>(self, name, logic, push_closure)
            .log_arrangement_size()
    }
}

/// Pins a closure to the higher-ranked signature `reduce_core` expects, which closure inference
/// cannot derive from `CursorTactic::new` alone.
fn reduce_core_logic<T1, T2, KC, F>(logic: F) -> F
where
    T1: TraceReader<Batch: Navigable>,
    T2: TraceReader<Batch: Navigable>,
    KC: BatchContainer,
    BatchCursor<T1>: Cursor<KeyContainer = KC>,
    for<'a> BatchCursor<T1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    F: FnMut(
            KC::ReadItem<'_>,
            &[(BatchVal<'_, T1>, BatchDiff<T1>)],
            &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
            &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
        ) + 'static,
{
    logic
}
