package ingest

import "testing"

func TestParseSyslog5424(t *testing.T) {
	f := map[string]string{}
	_, ok, _ := parseSyslogInto(`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - 'su root' failed for lonvick`, f)
	if !ok {
		t.Fatal("5424: no timestamp parsed")
	}
	// PRI 34 = facility 4, severity 2 (crit).
	want := map[string]string{
		"facility": "4",
		"severity": "crit",
		"hostname": "mymachine.example.com",
		"app_name": "su",
		"msg_id":   "ID47",
		"_msg":     "'su root' failed for lonvick",
	}
	for k, v := range want {
		if f[k] != v {
			t.Errorf("5424 %s = %q want %q", k, f[k], v)
		}
	}
	if _, present := f["proc_id"]; present {
		t.Errorf("5424 proc_id should be absent (-), got %q", f["proc_id"])
	}
}

func TestParseSyslog5424StructuredData(t *testing.T) {
	f := map[string]string{}
	parseSyslogInto(`<165>1 2003-10-11T22:14:15.003Z host evntslog - ID47 [exampleSDID@32473 iut="3"] an application event`, f)
	if f["structured_data"] != `[exampleSDID@32473 iut="3"]` {
		t.Errorf("structured_data = %q", f["structured_data"])
	}
	if f["_msg"] != "an application event" {
		t.Errorf("_msg = %q want the message after the SD", f["_msg"])
	}
}

func TestParseSyslog3164(t *testing.T) {
	f := map[string]string{}
	parseSyslogInto(`<13>Oct 11 22:14:15 host1 myapp: hello world`, f)
	// PRI 13 = facility 1, severity 5 (notice).
	if f["severity"] != "notice" || f["facility"] != "1" {
		t.Errorf("3164 pri: facility=%q severity=%q", f["facility"], f["severity"])
	}
	if f["hostname"] != "host1" {
		t.Errorf("3164 hostname = %q", f["hostname"])
	}
	if f["app_name"] != "myapp" {
		t.Errorf("3164 app_name = %q", f["app_name"])
	}
	if f["_msg"] != "hello world" {
		t.Errorf("3164 _msg = %q", f["_msg"])
	}
}
