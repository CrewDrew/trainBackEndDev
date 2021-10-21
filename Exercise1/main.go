package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

type client chan<- string

var (
	entering = make(chan client)
	leaving  = make(chan client)
	messages = make(chan string)
)

func main() {
	listener, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}

	go broadcaster()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConn(conn)
	}
}

func broadcaster() {
	clients := make(map[client]bool)
	for {
		select {
		case msg := <-messages:
			for cli := range clients {
				cli <- msg
			}

		case cli := <-entering:
			clients[cli] = true

		case cli := <-leaving:
			delete(clients, cli)
			close(cli)
		}
	}
}

func handleConn(conn net.Conn) {
	//var who string = ""
	ch := make(chan string)
	go clientWriter(conn, ch)
	go serverWriter(conn)

	ch <- "Enter your nickname:"
	answer := bufio.NewScanner(conn)
	who := ""
	if answer.Scan() {
		who = answer.Text()
	}

	if who == "" {
		who = conn.RemoteAddr().String()
	}
	ch <- "You are " + who

	messages <- who + " has arrived"
	entering <- ch

	input := bufio.NewScanner(conn)
	for input.Scan() {
		messages <- who + ": " + input.Text()
	}
	leaving <- ch
	messages <- who + " has left"
	conn.Close()
}

func clientWriter(conn net.Conn, ch <-chan string) {
	for msg := range ch {
		fmt.Fprintln(conn, msg)
	}
}

func serverWriter(conn net.Conn) {
	data := make([]byte, 8)
	n, err := os.Stdin.Read(data)
	if err == nil && n > 0 {
		messages <- "Server say: " + string(data)
	} else {
		return
	}
}
