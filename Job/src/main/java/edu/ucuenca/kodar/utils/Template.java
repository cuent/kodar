/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

/**
 *
 * @author cuent
 */
public class Template {

    private String cluster;
    private String kw;
    private String author;
    private String title;
    private String uriAuthor;
    private String uriPublication;

    public Template(String cluster, String kw, String author, String title, String uri, String uriPublication) {
        this.cluster = cluster;
        this.kw = kw;
        this.author = author;
        this.title = title;
        this.uriAuthor = uri;
        this.uriPublication = uriPublication;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getKw() {
        return kw;
    }

    public void setKw(String kw) {
        this.kw = kw;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUriAuthor() {
        return uriAuthor;
    }

    public void setUriAuthor(String uriAuthor) {
        this.uriAuthor = uriAuthor;
    }

    public String getUriPublication() {
        return uriPublication;
    }

    public void setUriPublication(String uriPublication) {
        this.uriPublication = uriPublication;
    }

}
