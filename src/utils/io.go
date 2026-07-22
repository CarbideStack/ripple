/*
 * Copyright (c) 2026 Daniel Bergløv
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * Provides small stdout and stderr printing helpers with optional formatting.
 */
package utils

import (
	"fmt"
	"os"
)

/**
 * Println writes one line to standard output.
 *
 * If additional arguments are supplied and format is a string,
 * the output is formatted using fmt.Fprintf. Otherwise the value
 * is written using fmt.Fprintln.
 *
 * @param format
 *   Value or format string to write.
 *
 * @param args
 *   Optional format arguments.
 *
 * @return
 *   Number of bytes written.
 *
 * @return
 *   Any write error.
 */
func Println(format any, args ...any) (int, error) {
	str, ok := format.(string)

	if !ok || len(args) == 0 {
		return fmt.Fprintln(os.Stdout, format)
	}

	return fmt.Fprintf(os.Stdout, str+"\n", args...)
}

/**
 * EPrintln writes one line to standard error.
 *
 * If additional arguments are supplied and format is a string,
 * the output is formatted using fmt.Fprintf. Otherwise the value
 * is written using fmt.Fprintln.
 *
 * @param format
 *   Value or format string to write.
 *
 * @param args
 *   Optional format arguments.
 *
 * @return
 *   Number of bytes written.
 *
 * @return
 *   Any write error.
 */
func EPrintln(format any, args ...any) (int, error) {
	str, ok := format.(string)

	if !ok || len(args) == 0 {
		return fmt.Fprintln(os.Stderr, format)
	}

	return fmt.Fprintf(os.Stderr, str+"\n", args...)
}
