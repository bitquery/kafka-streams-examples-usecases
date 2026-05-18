package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func writeProtoReflectTree(w io.Writer, msg protoreflect.Message, indent int) {
	if !msg.IsValid() {
		return
	}

	sp := strings.Repeat(" ", indent)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())

		switch {
		case fd.IsList():
			fmt.Fprintf(w, "%s%s (repeated):\n", sp, name)
			list := v.List()
			listKind := fd.Kind()

			for i := 0; i < list.Len(); i++ {
				ev := list.Get(i)
				switch listKind {
				case protoreflect.MessageKind:
					fmt.Fprintf(w, "%s  [%d]:\n", sp, i)
					writeProtoReflectTree(w, ev.Message(), indent+4)

				case protoreflect.BytesKind:
					fmt.Fprintf(w, "%s  [%d]: %s\n", sp, i, base58.Encode(ev.Bytes()))

				default:
					fmt.Fprintf(w, "%s  [%d]: %v\n", sp, i, formatProtoreflectValue(ev))
				}
			}

		case fd.IsMap():
			fmt.Fprintf(w, "%s%s (map):\n", sp, name)
			valueDesc := fd.MapValue()
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				keyStr := formatMapKey(k)
				fmt.Fprintf(w, "%s  %s => ", sp, keyStr)
				writeMapOrScalarValue(w, valueDesc, mv, indent+2)
				fmt.Fprintln(w)
				return true
			})

		case fd.Kind() == protoreflect.MessageKind:
			fmt.Fprintf(w, "%s%s:\n", sp, name)
			writeProtoReflectTree(w, v.Message(), indent+4)

		case fd.Kind() == protoreflect.BytesKind:
			fmt.Fprintf(w, "%s%s: %s\n", sp, name, base58.Encode(v.Bytes()))

		default:
			fmt.Fprintf(w, "%s%s: %v\n", sp, name, formatProtoreflectValue(v))
		}

		return true
	})
}

func formatProtoreflectValue(v protoreflect.Value) interface{} {
	if !v.IsValid() {
		return nil
	}
	return v.Interface()
}

func formatMapKey(k protoreflect.MapKey) string {
	if !k.IsValid() {
		return "<nil>"
	}
	switch v := k.Interface().(type) {
	case []byte:
		return base58.Encode(v)
	default:
		return fmt.Sprint(v)
	}
}

func writeMapOrScalarValue(w io.Writer, vd protoreflect.FieldDescriptor, v protoreflect.Value, nestedIndent int) {
	switch vd.Kind() {
	case protoreflect.MessageKind:
		fmt.Fprintln(w)
		writeProtoReflectTree(w, v.Message(), nestedIndent+2)
	case protoreflect.BytesKind:
		fmt.Fprintf(w, "%s", base58.Encode(v.Bytes()))
	default:
		fmt.Fprintf(w, "%v", formatProtoreflectValue(v))
	}
}
