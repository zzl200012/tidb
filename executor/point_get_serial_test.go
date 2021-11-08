// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func mustExecDDL(tk *testkit.TestKit, t *testing.T, sql string) {
	tk.MustExec(sql)
	require.NoError(t, domain.GetDomain(tk.Session()).Reload())
}

func TestMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Simply check the cached results.
	mustExecDDL(tk, t, "lock tables point read")
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables")

	cases := []struct {
		sql string
		r1  bool
		r2  bool
	}{
		{"explain analyze select * from point where id = 1", false, false},
		{"explain analyze select * from point where id in (1, 2)", false, false},

		// Cases for not exist keys.
		{"explain analyze select * from point where id = 3", true, true},
		{"explain analyze select * from point where id in (1, 3)", true, true},
		{"explain analyze select * from point where id in (3, 4)", true, true},
	}

	for _, ca := range cases {
		mustExecDDL(tk, t, "lock tables point read")

		rows := tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows), fmt.Sprintf("%v", ca.sql))
		explain := fmt.Sprintf("%v", rows[0])
		require.Regexp(t, explain, ".*num_rpc.*")

		rows = tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows))
		explain = fmt.Sprintf("%v", rows[0])
		ok := strings.Contains(explain, "num_rpc")
		require.Equal(t, ok, ca.r1, fmt.Sprintf("%v", ca.sql))
		mustExecDDL(tk, t, "unlock tables")

		rows = tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows))
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, explain, ".*num_rpc.*")

		// Test cache release after unlocking tables.
		mustExecDDL(tk, t, "lock tables point read")
		rows = tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows))
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, explain, ".*num_rpc.*")

		rows = tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows))
		explain = fmt.Sprintf("%v", rows[0])
		ok = strings.Contains(explain, "num_rpc")
		require.Equal(t, ok, ca.r2, fmt.Sprintf("%v", ca.sql))

		mustExecDDL(tk, t, "unlock tables")
		mustExecDDL(tk, t, "lock tables point read")

		rows = tk.MustQuery(ca.sql).Rows()
		require.Equal(t, 1, len(rows))
		explain = fmt.Sprintf("%v", rows[0])
		ok = strings.Contains(explain, "num_rpc")

		mustExecDDL(tk, t, "unlock tables")
	}
}

func TestPartitionMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int unique key, c int, d varchar(10)) partition by hash (id) partitions 4")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Confirm _tidb_rowid will not be duplicated.
	tk.MustQuery("select distinct(_tidb_rowid) from point order by _tidb_rowid").Check(testkit.Rows("1", "2"))

	mustExecDDL(tk, t, "lock tables point read")

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables")

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	tk.MustExec("update point set id = -id")

	// Test cache release after unlocking tables.
	mustExecDDL(tk, t, "lock tables point read")
	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows())

	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -2").Check(testkit.Rows("2"))

	mustExecDDL(tk, t, "unlock tables")
}
