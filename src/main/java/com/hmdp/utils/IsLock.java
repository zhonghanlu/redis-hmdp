package com.hmdp.utils;

public interface IsLock {

    boolean tryLock(long timeoutSec);

    void unlock();

}
