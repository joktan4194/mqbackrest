package config

import "testing"

// The shipped example config must always parse.
func TestExampleConfigParses(t *testing.T) {
	for _, v := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "MQBACKREST_CIPHER_PASS", "MQBACKREST_MYSQL_PASSWORD"} {
		t.Setenv(v, "x")
	}
	if _, err := Load("../../mqbackrest.example.yaml"); err != nil {
		t.Fatal(err)
	}
}
