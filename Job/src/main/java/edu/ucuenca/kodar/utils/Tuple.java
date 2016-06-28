/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Tuple<T0, T1> {

    public final T0 a;
    public final T1 b;

    public Tuple(T0 a, T1 b) {
        this.a = a;
        this.b = b;
    }

    public T0 getA() {
        return a;
    }

    public T1 getB() {
        return b;
    }

}
