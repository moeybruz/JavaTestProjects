package model;

@FunctionalInterface
public interface WriterRunnable<T, U> {

    void run(T t, U u);

}
