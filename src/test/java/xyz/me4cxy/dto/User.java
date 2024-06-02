package xyz.me4cxy.dto;

import java.util.List;

/**
 * @author jayin
 * @since 2024/05/15
 */
public class User {
    public User() {
    }

    public User(String pwd) {
        this.pwd = pwd;
    }

    private String usr;
    private String pwd;
    private List<Address> address;
    private Address mainAddr;

    public Address getMainAddr() {
        return mainAddr;
    }

    public void setMainAddr(Address mainAddr) {
        this.mainAddr = mainAddr;
    }

    public List<Address> getAddress() {
        return address;
    }

    public void setAddress(List<Address> address) {
        this.address = address;
    }

    public String getUsr() {
        return usr;
    }

    public void setUsr(String usr) {
        this.usr = usr;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    @Override
    public String toString() {
        return "User{" + "usr='" + usr + '\'' + ", pwd='" + pwd + '\'' + ", address=" + address + '}';
    }
}
