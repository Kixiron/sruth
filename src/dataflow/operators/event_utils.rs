use crossbeam_channel::{Receiver, Sender};
use std::{
    mem,
    ops::{Deref, DerefMut},
};
use timely::dataflow::operators::capture::{Event, EventPusher, Extract};

#[derive(Debug)]
pub struct CrossbeamPusher<T>(pub Sender<T>);

impl<T> CrossbeamPusher<T> {
    pub fn new(sender: Sender<T>) -> Self {
        Self(sender)
    }
}

impl<T, D> EventPusher<T, D> for CrossbeamPusher<Event<T, D>> {
    fn push(&mut self, event: Event<T, D>) {
        // Ignore any errors that occur
        let _ = self.send(event);
    }
}

impl<T> Clone for CrossbeamPusher<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Deref for CrossbeamPusher<T> {
    type Target = Sender<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CrossbeamPusher<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct CrossbeamExtractor<T>(pub Receiver<T>);

impl<T> CrossbeamExtractor<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        Self(receiver)
    }
}

impl<T: Ord, D: Ord> Extract<T, D> for CrossbeamExtractor<Event<T, D>> {
    fn extract(self) -> Vec<(T, Vec<D>)> {
        let mut result = Vec::new();
        for event in self {
            if let Event::Messages(time, data) = event {
                result.push((time, data));
            }
        }
        result.sort_by(|x, y| x.0.cmp(&y.0));

        let mut current = 0;
        for i in 1..result.len() {
            if result[current].0 == result[i].0 {
                let data = mem::take(&mut result[i].1);
                result[current].1.extend(data);
            } else {
                current = i;
            }
        }

        for &mut (_, ref mut data) in &mut result {
            data.sort();
        }
        result.retain(|x| !x.1.is_empty());
        result
    }
}

impl<T> IntoIterator for CrossbeamExtractor<T> {
    type IntoIter = crossbeam_channel::IntoIter<T>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> Clone for CrossbeamExtractor<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Deref for CrossbeamExtractor<T> {
    type Target = Receiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CrossbeamExtractor<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
