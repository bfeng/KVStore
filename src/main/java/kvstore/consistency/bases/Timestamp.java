package kvstore.consistency.bases;

public abstract class Timestamp{
    abstract public String genKey();
    abstract public int minus(Timestamp timestamp);
}