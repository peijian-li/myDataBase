package storage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache <K,V>{

    @NoArgsConstructor
    public class Node{
        K key;
        V value;
        Node pre;
        Node next;

        Node(K key,V value){
            this.key=key;
            this.value=value;
        }
    }

    private Map<K,Node> cache = new ConcurrentHashMap<K,Node>();
    private int size;
    private int capacity;
    private Node head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        head = new Node();
        tail = new Node();
        head.next = tail;
        head.pre = tail;
        tail.next = head;
        tail.pre = head;
    }

    public int getSize() {
        return size;
    }

    public Node getHead(){
        return head;
    }

    public Node getTail(){
        return tail;
    }

    public Map<K,Node> getCache(){
        return cache;
    }

    public synchronized V get(K key){
        Node node = cache.get(key);
        if(node==null){
            return null;
        }
        moveToHead(node);
        return node.value;
    }

    public synchronized void put(K key,V value){
        Node node = cache.get(key);
        if(node!=null){
            node.value=value;
            moveToHead(node);
        }else{
            Node newNode = new Node(key, value);
            addToHead(newNode);
            cache.put(key,newNode);
            size++;
            if(size>capacity){
                remove(tail.pre);
            }
        }
    }

    public synchronized void remove(Node node){
        removeNode(node);
        cache.remove(node.key);
        size--;
    }

    private void moveToHead(Node node){
        removeNode(node);
        addToHead(node);
    }


    private void removeNode(Node node){
        node.pre.next=node.next;
        node.next.pre=node.pre;
    }

    public void addToHead(Node node){
        node.pre=head;
        node.next=head.next;
        head.next=node;
        node.next.pre=node;
    }


}
