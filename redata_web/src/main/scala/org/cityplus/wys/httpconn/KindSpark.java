package org.cityplus.wys.httpconn;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-10-07-下午5:52
 */
public class KindSpark {

    private String id;
    private String name;
    private String appId;
    private String owner;
    private String proxyUser;
    private String state;
    private String kind;

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getAppId() {
        return appId;
    }

    public String getOwner() {
        return owner;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public String getState() {
        return state;
    }

    public String getKind() {
        return kind;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }
}
