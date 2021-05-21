use std::iter::{Peekable, FromIterator};
use std::marker::PhantomData;

pub struct Buffering<I, U, F>
where
    I: Iterator<Item = U>,
    F: FromIterator<U>,
{
    iter: Peekable<I>,
    size: usize,
    f: PhantomData<F>,
}

impl<I, U, F> Iterator for Buffering<I, U, F>
where
    I: Iterator<Item = U>,
    F: FromIterator<U>,
{
    type Item = F;
    fn next(&mut self) -> Option<Self::Item> {
        if self.iter.peek().is_none() {
            return None
        }
        
        let iter = &mut self.iter;
        Some(iter.take(self.size).collect::<F>())
    }
}

pub trait BufferingBuilder<I, U>
where
    I: Iterator<Item = U>,
{
    fn buffering<F>(self, size: usize) -> Buffering<I, U, F>
    where
        F: FromIterator<U>;
}

impl<I, U> BufferingBuilder<I, U> for I
where
    I: Iterator<Item = U>,
{
    fn buffering<F>(self, size: usize) -> Buffering<I, I::Item, F>
    where
        F: FromIterator<U>,
    {
        Buffering::<I, I::Item, F> {
            iter: self.peekable(),
            f: PhantomData,
            size: size,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_buffering() {
        let v = vec![1, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]],
            v.into_iter()
                .buffering::<Vec<u32>>(3)
                .collect::<Vec<Vec<u32>>>()
        );
    }
}
