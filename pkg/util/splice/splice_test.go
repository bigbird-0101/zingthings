package splice

import "testing"

func TestContains(t *testing.T) {
	type args[T any] struct {
		array   []T
		element T
	}
	type testCase[T any] struct {
		name string
		args args[T]
		want bool
	}
	tests := []testCase[any]{
		{
			name: "test",
			args: args[any]{
				array:   []any{1, 2, 3, 4, 5},
				element: 2,
			},
			want: true,
		},
		{
			name: "test",
			args: args[any]{
				array:   []any{1, 2, 3, 4, 5},
				element: 6,
			},
			want: false,
		},
		{
			name: "test2",
			args: args[any]{
				array:   []any{"test", "testc"},
				element: "testB",
			},
			want: false,
		},
		{
			name: "test2",
			args: args[any]{
				array:   []any{"test", "testc"},
				element: "test",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Contains(tt.args.array, tt.args.element); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}
