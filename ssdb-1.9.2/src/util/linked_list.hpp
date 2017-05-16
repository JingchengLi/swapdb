//
// Created by zts on 17-5-16.
//

#ifndef SSDB_LINKEDLIST_H
#define SSDB_LINKEDLIST_H

namespace r2m {


    template<typename T>
    class Node {
    public:
        T *data;
        Node *next = nullptr;
        Node *prev = nullptr;
    };


    template<typename T>
    class LinkedList {
    public:
        void push_back(T *newNode);

        int remove(T *oldNode);

        LinkedList() = default;

        virtual ~LinkedList();

        int size = 0;


        class Iterator {
        private:
            Node<T> *p = nullptr;
        public:
            friend class LinkedList;

            Node<T> *next() {
                Node<T> *ret = p;
                if (p != nullptr) {
                    p = p->next;
                }
                return ret;
            }
        };

        friend class Iterator;


        Iterator iterator() {
            Iterator it;
            it.p = this->head;
            return it;
        }


    private:
        Node<T> *head = nullptr;
        Node<T> *tail = nullptr;
    };


    template<typename T>
    void LinkedList<T>::push_back(T *newNode) {
        this->size++;

        Node<T> *node = new Node<T>();
        node->data = newNode;
        node->prev = tail;

        if (this->tail == nullptr) {
            this->head = node;
        } else {
            this->tail->next = node;
        }

        this->tail = node;
    }

    template<typename T>
    int LinkedList<T>::remove(T *oldNode) {
        Node<T> *current = head;
        while (current != nullptr) {
            if (oldNode == current->data) {
                break;
            }
            current = current->next;
        }
        if (current == nullptr) {
            return 0;
        }

        this->size--;

        if (current->prev != nullptr) {
            current->prev->next = current->next;
        }

        if (current->next != nullptr) {
            current->next->prev = current->prev;
        }

        if (this->head == current) {
            this->head = current->next;
        }

        if (this->tail == current) {
            this->tail = current->prev;
        }

        delete current;

        return 1;
    }

    template<typename T>
    LinkedList<T>::~LinkedList() {
        Node<T> *start = head;
        Node<T> *current = nullptr;

        while (start != nullptr) {
            current = start;
            start = start->next;
            delete current;
        }
    }


}

#endif //SSDB_LINKEDLIST_H
