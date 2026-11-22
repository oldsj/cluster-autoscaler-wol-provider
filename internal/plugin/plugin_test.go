package plugin

import "testing"

func TestRenderFields(t *testing.T) {
	tp, err := ParseTemplate("body", `{{.Name}} {{.Node}} {{.Group}} {{.Action}} {{.DryRun}}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		dry  bool
		want string
	}{{false, "m n g approve false"}, {true, "m n g approve true"}} {
		got, err := tp.Render(Target{Name: "m", Node: "n", Group: "g", DryRun: c.dry}, map[string]string{"Action": "approve"})
		if err != nil || got != c.want {
			t.Fatalf("Render = %q, %v; want %q", got, err, c.want)
		}
	}
}
