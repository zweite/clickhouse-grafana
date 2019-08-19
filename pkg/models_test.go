package main

import "testing"

func Test_convertInterval(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "5m",
			args: args{
				str: "5m",
			},
			want: 5 * 60,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertInterval(tt.args.str); got != tt.want {
				t.Errorf("convertInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}
