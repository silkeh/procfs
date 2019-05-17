// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btrfs

import "testing"

func TestFSBtrfsStats(t *testing.T) {
	btrfs, err := NewFS("../fixtures/sys")
	if err != nil {
		t.Fatalf("failed to access Btrfs filesystem: %v", err)
	}
	stats, err := btrfs.Stats()
	if err != nil {
		t.Fatalf("failed to parse Btrfs stats: %v", err)
	}

	tests := []struct {
		uuid, label        string
		devices, features  int
		data, meta, system uint64
	}{
		{
			uuid:     "0abb23a9-579b-43e6-ad30-227ef47fcb9d",
			label:    "fixture",
			devices:  2,
			features: 4,
			data:     2147483648,
			meta:     1073741824,
			system:   8388608,
		},
	}

	const expect = 1

	if l := len(stats); l != expect {
		t.Fatalf("unexpected number of btrfs stats: %d", l)
	}
	if l := len(tests); l != expect {
		t.Fatalf("unexpected number of tests: %d", l)
	}

	for i, tt := range tests {
		if want, got := tt.uuid, stats[i].UUID; want != got {
			t.Errorf("unexpected stats name:\nwant: %q\nhave: %q", want, got)
		}

		if want, got := tt.devices, len(stats[i].Devices); want != got {
			t.Errorf("unexpected number of devices:\nwant: %d\nhave: %d", want, got)
		}

		if want, got := tt.features, len(stats[i].Features); want != got {
			t.Errorf("unexpected number of features:\nwant: %d\nhave: %d", want, got)
		}

		if want, got := tt.data, stats[i].Allocation.Data.TotalBytes; want != got {
			t.Errorf("unexpected data size:\nwant: %d\nhave: %d", want, got)
		}

		if want, got := tt.meta, stats[i].Allocation.Metadata.TotalBytes; want != got {
			t.Errorf("unexpected metadata size:\nwant: %d\nhave: %d", want, got)
		}

		if want, got := tt.system, stats[i].Allocation.System.TotalBytes; want != got {
			t.Errorf("unexpected data size:\nwant: %d\nhave: %d", want, got)
		}
	}
}
