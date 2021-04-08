use std::cmp::{self, Ordering};
use timely::dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream};

// TODO: Use traces to do this efficiently for ddflow
pub trait UnionFind {
    type Output;

    fn find(&self) -> Self::Output;
}

impl<S> UnionFind for Stream<S, (usize, usize)>
where
    S: Scope,
{
    type Output = Stream<S, (usize, usize)>;

    fn find(&self) -> Stream<S, (usize, usize)> {
        self.unary(Pipeline, "UnionFind", |_capabilities, _info| {
            let mut roots = vec![]; // u32 works, and is smaller than uint/u64
            let mut ranks = vec![]; // u8 should be large enough (n < 2^256)

            move |input, output| {
                input.for_each(|time, data| {
                    let mut session = output.session(&time);

                    for &(mut x, mut y) in data.iter() {
                        // grow arrays if required.
                        let max = cmp::max(x, y);
                        roots.reserve(max + 1);
                        ranks.reserve(max + 1);

                        for idx in roots.len()..(max + 1) {
                            roots.push(idx);
                            ranks.push(0);
                        }

                        // look up roots for `x` and `y`.
                        while x != roots[x] {
                            x = roots[x];
                        }
                        while y != roots[y] {
                            y = roots[y];
                        }

                        if x != y {
                            session.give((x, y));

                            match ranks[x].cmp(&ranks[y]) {
                                Ordering::Less => roots[x] = y,

                                Ordering::Greater => roots[y] = x,

                                Ordering::Equal => {
                                    roots[y] = x;
                                    ranks[x] += 1
                                }
                            }
                        }
                    }
                });
            }
        })
    }
}
