package com.suse.salt.netapi.results;

public class FullReturn<T> {

    private T ret;
    private int retcode;
    private String jid;

    public int getRetcode() {
        return retcode;
    }

    public String getJid() {
        return jid;
    }

    public T getRet() {
        return ret;
    }

    @Override
    public String toString() {
        return "FullReturn{" + "ret=" + ret + ", retcode=" + retcode + ", jid='" + jid + '\'' + '}';
    }
}
