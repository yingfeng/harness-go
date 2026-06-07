// This example demonstrates different channel types.
package main

import (
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness"
	"github.com/infiniflow/ragflow/harness/graphengine/channels"
)

func main() {
	fmt.Println("=== LastValue Channel ===")
	lastValueDemo()

	fmt.Println("\n=== Topic Channel ===")
	topicDemo()

	fmt.Println("\n=== BinaryOperatorAggregate Channel ===")
	binopDemo()

	fmt.Println("\n=== EphemeralValue Channel ===")
	ephemeralDemo()

	fmt.Println("\n=== All demos completed! ===")
}

func lastValueDemo() {
	ch := harness.NewLastValue("")
	ch.SetKey("last_message")

	// Update with single value
	updated, err := ch.Update([]interface{}{"Hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated: %v\n", updated)

	// Update with another value
	updated, err = ch.Update([]interface{}{"World"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated: %v\n", updated)

	// Get value (should be "World" - the last one)
	val, err := ch.Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("LastValue: %v\n", val)
}

func topicDemo() {
	ch := harness.NewTopic("", false) // Don't accumulate
	ch.SetKey("messages")

	// Update with values
	ch.Update([]interface{}{"Hello"})
	ch.Update([]interface{}{"World", "!"})

	// Get values
	val, err := ch.Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Topic values: %v\n", val)

	// Update again - previous values are cleared since accumulate=false
	ch.Update([]interface{}{"New message"})
	val, _ = ch.Get()
	fmt.Printf("Topic values after update: %v\n", val)
}

func binopDemo() {
	// Create a channel that sums integers
	ch := channels.NewBinaryOperatorAggregate(0, func(a, b interface{}) interface{} {
		ai := a.(int)
		bi := b.(int)
		return ai + bi
	})
	ch.SetKey("total")

	// Update with values
	ch.Update([]interface{}{10, 20, 30})

	// Get value (should be 60)
	val, err := ch.Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("BinaryOperator sum: %v\n", val)

	// Test with list append
	listCh := harness.NewBinaryOperatorAggregate([]interface{}{}, harness.ListAppend)
	listCh.SetKey("items")
	listCh.Update([]interface{}{[]interface{}{"a", "b"}})
	listCh.Update([]interface{}{[]interface{}{"c", "d"}})

	listVal, _ := listCh.Get()
	fmt.Printf("BinaryOperator list: %v\n", listVal)
}

func ephemeralDemo() {
	ch := harness.NewEphemeralValue("", true)
	ch.SetKey("temp")

	// Set value
	ch.Update([]interface{}{"temporary value"})

	// Get value (this will clear it)
	val, err := ch.Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Ephemeral value (first read): %v\n", val)

	// Try to get again (should be empty)
	_, err = ch.Get()
	if err != nil {
		fmt.Printf("Ephemeral value (second read): empty (error: %v)\n", err)
	}
}
