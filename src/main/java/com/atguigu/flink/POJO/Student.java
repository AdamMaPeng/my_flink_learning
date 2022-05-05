package com.atguigu.flink.POJO;

/**
 * @author Adam-Ma
 * @date 2022/5/3 22:08
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

/**
 *  Flink 中对 POJO 类的要求：
 *     1、公共的类 或 独立的无非静态内部类的类
 *     2、公共无参的构造器
 *     3、类中所有的字段都是public 和 非 final 的，或有 一个 getter ，setter 方法
 */
public class Student {
    public int id;
    public String name;
    public int age;

    public Student(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Student() {
    }
}
