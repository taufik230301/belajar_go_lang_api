// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"golang.org/x/sync/errgroup"
)

var _ godror.ObjectScanner = new(MyRecord)
var _ godror.ObjectWriter = new(MyRecord)

var _ godror.ObjectScanner = new(MyTable)

// MYRecord represents TEST_PKG_TYPES.MY_RECORD
type MyRecord struct {
	*godror.Object
	Txt string
	ID  int64
}

type coder interface{ Code() int }

func (r *MyRecord) Scan(src interface{}) error {
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	id, err := obj.Get("ID")
	if err != nil {
		return err
	}
	r.ID = id.(int64)

	txt, err := obj.Get("TXT")
	if err != nil {
		return err
	}
	r.Txt = string(txt.([]byte))

	return nil
}

// WriteObject update godror.Object with struct attributes values.
// Implement this method if you need the record as an input parameter.
func (r MyRecord) WriteObject() error {
	// all attributes must be initialized or you get an "ORA-21525: attribute number or (collection element at index) %s violated its constraints"
	err := r.ResetAttributes()
	if err != nil {
		return err
	}

	var data godror.Data
	err = r.GetAttribute(&data, "ID")
	if err != nil {
		return err
	}
	data.SetInt64(r.ID)
	r.SetAttribute("ID", &data)

	if r.Txt != "" {
		err = r.GetAttribute(&data, "TXT")
		if err != nil {
			return err
		}

		data.SetBytes([]byte(r.Txt))
		r.SetAttribute("TXT", &data)
	}

	return nil
}

// MYTable represents TEST_PKG_TYPES.MY_TABLE
type MyTable struct {
	godror.ObjectCollection
	Items []*MyRecord
	conn  interface {
		NewData(baseType interface{}, sliceLen, bufSize int) ([]*godror.Data, error)
	}
}

func (t *MyTable) Scan(src interface{}) error {
	//fmt.Printf("Scan(%T(%#v))\n", src, src)
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	collection := obj.Collection()
	length, err := collection.Len()
	//fmt.Printf("Collection[%d] %#v: %+v\n", length, collection, err)
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	t.Items = make([]*MyRecord, 0, length)
	var i int
	for i, err = collection.First(); err == nil; i, err = collection.Next(i) {
		//fmt.Printf("Scan[%d]: %+v\n", i, err)
		var data godror.Data
		err = collection.GetItem(&data, i)
		if err != nil {
			return err
		}

		o := data.GetObject()
		defer o.Close()
		//fmt.Printf("%d. data=%#v => o=%#v\n", i, data, o)

		var item MyRecord
		err = item.Scan(o)
		//fmt.Printf("%d. item=%#v: %+v\n", i, item, err)
		if err != nil {
			return err
		}
		t.Items = append(t.Items, &item)
	}
	if err == godror.ErrNotExist {
		return nil
	}
	return err
}

func (r MyTable) WriteObject(ctx context.Context) error {
	if len(r.Items) == 0 {
		return nil
	}

	data, err := r.conn.NewData(r.Items[0], len(r.Items), 0)
	if err != nil {
		return err
	}

	for i, item := range r.Items {
		err = item.WriteObject()
		if err != nil {
			return err
		}
		d := data[i]
		d.SetObject(item.ObjectRef())
		r.Append(d)
	}
	return nil
}

func createPackages(ctx context.Context) error {
	qry := []string{`CREATE OR REPLACE PACKAGE test_pkg_types AS
	TYPE my_other_record IS RECORD (
		id    NUMBER(5),
		txt   VARCHAR2(200)
	);
	TYPE my_record IS RECORD (
		id    NUMBER(5),
		other test_pkg_types.my_other_record,
		txt   VARCHAR2(200)
	);
	TYPE my_table IS
		TABLE OF my_record;
	END test_pkg_types;`,

		`CREATE OR REPLACE PACKAGE test_pkg_sample AS
	PROCEDURE test_record (
		id    IN    NUMBER,
		txt   IN    VARCHAR,
		rec   OUT   test_pkg_types.my_record
	);

	PROCEDURE test_record_in (
		rec IN OUT test_pkg_types.my_record
	);

	FUNCTION test_table (
		x NUMBER
	) RETURN test_pkg_types.my_table;

	PROCEDURE test_table_in (
		tb IN OUT test_pkg_types.my_table
	);

	END test_pkg_sample;`,

		`CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS

	PROCEDURE test_record (
		id    IN    NUMBER,
		txt   IN    VARCHAR,
		rec   OUT   test_pkg_types.my_record
	) IS
	BEGIN
		rec.id := id;
		rec.txt := txt;
	END test_record;

	PROCEDURE test_record_in (
		rec IN OUT test_pkg_types.my_record
	) IS
	BEGIN
		rec.id := rec.id + 1;
		rec.txt := rec.txt || ' changed';
	END test_record_in;

	FUNCTION test_table (
		x NUMBER
	) RETURN test_pkg_types.my_table IS
		tb     test_pkg_types.my_table;
		item   test_pkg_types.my_record;
	BEGIN
		tb := test_pkg_types.my_table();
		FOR c IN (
			SELECT
				level "LEV"
			FROM
				"SYS"."DUAL" "A1"
			CONNECT BY
				level <= x
		) LOOP
			item.id := c.lev;
			item.txt := 'test - ' || ( c.lev * 2 );
			tb.extend();
			tb(tb.count) := item;
		END LOOP;

		RETURN tb;
	END test_table;

	PROCEDURE test_table_in (
		tb IN OUT test_pkg_types.my_table
	) IS
	BEGIN
	null;
	END test_table_in;

	END test_pkg_sample;`}

	for _, ddl := range qry {
		_, err := testDb.ExecContext(ctx, ddl)
		if err != nil {
			return err

		}
	}

	return nil
}

func dropPackages(ctx context.Context) {
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_types`)
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_sample`)
}

func TestPlSqlTypes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(testContext("PLSQLTypes"), 30*time.Second)
	defer cancel()

	errOld := errors.New("client or server < 12")
	if err := godror.Raw(ctx, testDb, func(conn godror.Conn) error {
		serverVersion, err := conn.ServerVersion()
		if err != nil {
			return err
		}
		clientVersion, err := conn.ClientVersion()
		if err != nil {
			return err
		}

		if serverVersion.Version < 12 || clientVersion.Version < 12 {
			return errOld
		}
		return nil
	}); err != nil {
		if errors.Is(err, errOld) {
			t.Skip(err)
		} else {
			t.Fatal(err)
		}
	}

	if err := createPackages(ctx); err != nil {
		t.Fatal(err)
	}
	defer dropPackages(ctx)

	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Record", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		obj, err := objType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()

		for tName, tCase := range map[string]struct {
			txt  string
			want MyRecord
			ID   int64
		}{
			"default":    {ID: 1, txt: "test", want: MyRecord{Object: obj, ID: 1, Txt: "test"}},
			"emptyTxt":   {ID: 2, txt: "", want: MyRecord{Object: obj, ID: 2}},
			"zeroValues": {want: MyRecord{Object: obj}},
		} {
			rec := MyRecord{Object: obj}
			params := []interface{}{
				sql.Named("id", tCase.ID),
				sql.Named("txt", tCase.txt),
				sql.Named("rec", sql.Out{Dest: &rec}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_record(:id, :txt, :rec); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 21779 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if rec != tCase.want {
				t.Errorf("%s: record got %v, wanted %v", tName, rec, tCase.want)
			}
		}
	})

	t.Run("Record IN OUT", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		for tName, tCase := range map[string]struct {
			wantTxt string
			in      MyRecord
			wantID  int64
		}{
			"zeroValues": {in: MyRecord{}, wantID: 1, wantTxt: " changed"},
			"default":    {in: MyRecord{ID: 1, Txt: "test"}, wantID: 2, wantTxt: "test changed"},
			"emptyTxt":   {in: MyRecord{ID: 2, Txt: ""}, wantID: 3, wantTxt: " changed"},
		} {

			obj, err := objType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			rec := MyRecord{Object: obj, ID: tCase.in.ID, Txt: tCase.in.Txt}
			params := []interface{}{
				sql.Named("rec", sql.Out{Dest: &rec, In: true}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 21779 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if rec.ID != tCase.wantID {
				t.Errorf("%s: ID got %d, wanted %d", tName, rec.ID, tCase.wantID)
			}
			if rec.Txt != tCase.wantTxt {
				t.Errorf("%s: Txt got %s, wanted %s", tName, rec.Txt, tCase.wantTxt)
			}
		}
	})

	t.Run("Table", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}

		items := []*MyRecord{{ID: 1, Txt: "test - 2"}, {ID: 2, Txt: "test - 4"}}

		for tName, tCase := range map[string]struct {
			want MyTable
			in   int64
		}{
			"one": {in: 1, want: MyTable{Items: items[:1]}},
			"two": {in: 2, want: MyTable{Items: items}},
		} {

			obj, err := objType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			tb := MyTable{ObjectCollection: obj.Collection(), conn: conn}
			params := []interface{}{
				sql.Named("x", tCase.in),
				sql.Named("tb", sql.Out{Dest: &tb}),
			}
			_, err = cx.ExecContext(ctx, `begin :tb := test_pkg_sample.test_table(:x); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 30757 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if len(tb.Items) != len(tCase.want.Items) {
				t.Errorf("%s: table got %v items, wanted %d items", tName, tb.Items, len(tCase.want.Items))
			} else {
				for i := 0; i < len(tb.Items); i++ {
					got := tb.Items[i]
					want := tCase.want.Items[i]
					if got.ID != want.ID {
						t.Errorf("%s: record ID got %v, wanted %v", tName, got.ID, want.ID)
					}
					if got.Txt != want.Txt {
						t.Errorf("%s: record TXT got %v, wanted %v", tName, got.Txt, want.Txt)
					}
				}
			}
		}
	})

	t.Run("Table IN", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		tableObjType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}

		recordObjType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		items := make([]*MyRecord, 0)

		obj1, err := recordObjType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj1.Close()
		items = append(items, &MyRecord{ID: 1, Txt: "test - 2", Object: obj1})

		obj2, err := recordObjType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj2.Close()
		items = append(items, &MyRecord{ID: 2, Txt: "test - 4", Object: obj2})

		for tName, tCase := range map[string]struct {
			want MyTable
		}{
			"one": {want: MyTable{Items: items[:1]}},
			"two": {want: MyTable{Items: items}},
		} {

			obj, err := tableObjType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			tb := MyTable{ObjectCollection: obj.Collection(), Items: tCase.want.Items, conn: conn}
			params := []interface{}{
				sql.Named("tb", sql.Out{Dest: &tb, In: true}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_table_in(:tb); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 30757 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if len(tb.Items) != len(tCase.want.Items) {
				t.Errorf("%s: table got %v items, wanted %v items", tName, len(tb.Items), len(tCase.want.Items))
			} else {
				for i := 0; i < len(tb.Items); i++ {
					got := tb.Items[i]
					want := tCase.want.Items[i]
					if got.ID != want.ID {
						t.Errorf("%s: record ID got %v, wanted %v", tName, got.ID, want.ID)
					}
					if got.Txt != want.Txt {
						t.Errorf("%s: record TXT got %v, wanted %v", tName, got.Txt, want.Txt)
					}
				}
			}
		}
	})

}

func TestSelectObjectTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectObjectTable"), 30*time.Second)
	defer cancel()
	const objTypeName, objTableName, pkgName = "test_selectObject", "test_selectObjTab", "test_selectObjPkg"
	cleanup := func() {
		testDb.Exec("DROP PACKAGE " + pkgName)
		testDb.Exec("DROP TYPE " + objTableName)
		testDb.Exec("DROP TYPE " + objTypeName)
	}
	cleanup()
	for _, qry := range []string{
		`CREATE OR REPLACE TYPE ` + objTypeName + ` AS OBJECT (
    AA NUMBER(2),
    BB NUMBER(13,2),
    CC NUMBER(13,2),
    DD NUMBER(13,2),
    MSG varchar2(100))`,
		"CREATE OR REPLACE TYPE " + objTableName + " AS TABLE OF " + objTypeName,
		`CREATE OR REPLACE PACKAGE ` + pkgName + ` AS
    function FUNC_1( p1 in varchar2, p2 in varchar2) RETURN ` + objTableName + `;
    END;`,
		`CREATE OR REPLACE PACKAGE BODY ` + pkgName + ` AS
	FUNCTION func_1( p1 IN VARCHAR2, p2 IN VARCHAR2) RETURN ` + objTableName + ` is
    	ret ` + objTableName + ` := ` + objTableName + `();
    begin
		ret.extend;
		ret(ret.count):= ` + objTypeName + `( 11, 22, 33, 44, p1||'success!'||p2);
		ret.extend;
		ret(ret.count):= ` + objTypeName + `( 55, 66, 77, 88, p1||'failed!'||p2);
		return ret;
	end;
	END;`,
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Error(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer cleanup()

	const qry = "select " + pkgName + ".FUNC_1('aa','bb') from dual"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	for rows.Next() {
		var objI interface{}
		if err = rows.Scan(&objI); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		obj := objI.(*godror.Object).Collection()
		defer obj.Close()
		t.Log(obj.FullName())
		i, err := obj.First()
		if err != nil {
			t.Fatal(err)
		}
		var objData, attrData godror.Data
		for {
			if err = obj.GetItem(&objData, i); err != nil {
				t.Fatal(err)
			}
			if err = objData.GetObject().GetAttribute(&attrData, "MSG"); err != nil {
				t.Fatal(err)
			}
			msg := string(attrData.GetBytes())

			t.Logf("%d. msg: %+v", i, msg)

			if i, err = obj.Next(i); err != nil {
				break
			}
		}
	}
}

func TestFuncBool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("FuncBool"), 3*time.Second)
	defer cancel()
	const pkgName = "test_bool"
	cleanup := func() { testDb.Exec("DROP PROCEDURE " + pkgName) }
	cleanup()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = godror.EnableDbmsOutput(ctx, conn); err != nil {
		t.Error(err)
	}
	const crQry = "CREATE OR REPLACE PROCEDURE " + pkgName + `(p_in IN BOOLEAN, p_not OUT BOOLEAN, p_num OUT NUMBER) IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('in='||(CASE WHEN p_in THEN 'Y' ELSE 'N' END));
  p_not := NOT p_in;
  p_num := CASE WHEN p_in THEN 1 ELSE 0 END;
END;`
	if _, err = conn.ExecContext(ctx, crQry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", crQry, err))
	}
	defer cleanup()

	const qry = "BEGIN " + pkgName + "(p_in=>:1, p_not=>:2, p_num=>:3); END;"
	var buf bytes.Buffer
	for _, in := range []bool{true, false} {
		var out bool
		var num int
		if _, err = conn.ExecContext(ctx, qry, in, sql.Out{Dest: &out}, sql.Out{Dest: &num}); err != nil {
			if srv, err := godror.ServerVersion(ctx, conn); err != nil {
				t.Log(err)
			} else if srv.Version < 18 {
				t.Skipf("%q: %v", qry, err)
			} else {
				t.Errorf("%q: %v", qry, err)
			}
			continue
		}
		t.Logf("in:%v not:%v num:%v", in, out, num)
		want := 0
		if in {
			want = 1
		}
		if num != want || out != !in {
			buf.Reset()
			if err = godror.ReadDbmsOutput(ctx, &buf, conn); err != nil {
				t.Error(err)
			}
			t.Log(buf.String())
			t.Errorf("got %v/%v wanted %v/%v", out, num, want, !in)
		}
	}
}

func TestPlSqlObjectDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSqlObjectDirect"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}

	const crea = `CREATE OR REPLACE PACKAGE test_pkg_obj IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(10), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;

  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ, p_int IN PLS_INTEGER);
END;`
	const crea2 = `CREATE OR REPLACE PACKAGE BODY test_pkg_obj IS
  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ, p_int IN PLS_INTEGER) IS
    v_idx PLS_INTEGER := NVL(p_obj.LAST, 0) + 1;
  BEGIN
    p_obj(v_idx).int := p_int;
    p_obj(v_idx).num := 314/100;
	p_obj(v_idx).vc  := 'abraka';
	p_obj(v_idx).c   := 'X';
	p_obj(v_idx).dt  := SYSDATE;
  END modify;
END;`
	if err = prepExec(ctx, testCon, crea); err != nil {
		t.Fatal(err)
	}
	//defer prepExec(ctx, testCon, "DROP PACKAGE test_pkg_obj")
	if err = prepExec(ctx, testCon, crea2); err != nil {
		t.Fatal(err)
	}

	//defer tl.enableLogging(t)()
	clientVersion, _ := godror.ClientVersion(ctx, testDb)
	serverVersion, _ := godror.ServerVersion(ctx, testDb)
	t.Logf("clientVersion: %#v, serverVersion: %#v", clientVersion, serverVersion)
	cOt, err := testCon.GetObjectType(strings.ToUpper("test_pkg_obj.tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
		}
		t.Log(err)
		t.Skipf("client=%d or server=%d < 12", clientVersion.Version, serverVersion.Version)
	}
	t.Log(cOt)

	// create object from the type
	coll, err := cOt.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer coll.Close()

	// create an element object
	elt, err := cOt.CollectionOf.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer elt.Close()
	elt.ResetAttributes()
	if err = elt.Set("C", "Z"); err != nil {
		t.Fatal(err)
	}
	if err = elt.Set("INT", int32(-2)); err != nil {
		t.Fatal(err)
	}

	// append to the collection
	t.Logf("elt: %s", elt)
	coll.AppendObject(elt)

	const mod = "BEGIN test_pkg_obj.modify(:1, :2); END;"
	if err = prepExec(ctx, testCon, mod,
		driver.NamedValue{Ordinal: 1, Value: coll},
		driver.NamedValue{Ordinal: 2, Value: 42},
	); err != nil {
		t.Error(err)
	}
	t.Logf("coll: %s", coll)
	var data godror.Data
	for i, err := coll.First(); err == nil; i, err = coll.Next(i) {
		if err = coll.GetItem(&data, i); err != nil {
			t.Fatal(err)
		}
		elt.ResetAttributes()
		elt = data.GetObject()

		t.Logf("elt[%d]: %s", i, elt)
		for attr := range elt.Attributes {
			val, err := elt.Get(attr)
			if err != nil {
				if godror.DpiVersionNumber <= 30201 {
					t.Log(err, attr)
				} else {
					t.Error(err, attr)
				}
			}
			t.Logf("elt[%d].%s=%v", i, attr, val)
		}
	}
}
func prepExec(ctx context.Context, testCon driver.ConnPrepareContext, qry string, args ...driver.NamedValue) error {
	stmt, err := testCon.PrepareContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	_, err = stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	stmt.Close()
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	return nil
}

func TestPlSqlObject(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("PlSqlObject"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pkg := strings.ToUpper("test_pkg_obj" + tblSuffix)
	qry := `CREATE OR REPLACE PACKAGE ` + pkg + ` IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(1000), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;
END;`
	if _, err = conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP PACKAGE " + pkg)

	defer tl.enableLogging(t)()
	ot, err := godror.GetObjectType(ctx, conn, pkg+strings.ToUpper(".int_tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
		}
		t.Log(err)
		t.Skip("client or server version < 12")
	}
	t.Log(ot)
}

func TestCallWithObject(t *testing.T) {
	t.Parallel()
	cleanup := func() {
		for _, drop := range []string{
			"DROP PROCEDURE test_cwo_getSum",
			"DROP TYPE test_cwo_tbl_t",
			"DROP TYPE test_cwo_rec_t",
		} {
			testDb.Exec(drop)
		}
	}

	const crea = `CREATE OR REPLACE TYPE test_cwo_rec_t FORCE AS OBJECT (
  numberpart1 VARCHAR2(6),
  numberpart2 VARCHAR2(10),
  code VARCHAR(7),
  CONSTRUCTOR FUNCTION test_cwo_rec_t RETURN SELF AS RESULT
);

CREATE OR REPLACE TYPE test_cwo_tbl_t FORCE AS TABLE OF test_cwo_rec_t;

CREATE OR REPLACE PROCEDURE test_cwo_getSum(
  p_operation_id IN OUT VARCHAR2,
  a_languagecode_i IN VARCHAR2,
  a_username_i IN VARCHAR2,
  a_channelcode_i IN VARCHAR2,
  a_mcalist_i IN test_cwo_tbl_t,
  a_validfrom_i IN DATE,
  a_validto_i IN DATE,
  a_statuscode_list_i IN VARCHAR2 ,
  a_type_list_o OUT SYS_REFCURSOR
) IS
  cnt PLS_INTEGER;
BEGIN
  cnt := a_mcalist_i.COUNT;
  OPEN a_type_list_o FOR
    SELECT cnt FROM DUAL;
END;
`

	ctx, cancel := context.WithTimeout(testContext("CallWithObject"), time.Minute)
	defer cancel()

	cleanup()
	for _, qry := range strings.Split(crea, "CREATE OR") {
		if qry == "" {
			continue
		}
		qry = "CREATE OR" + qry
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}

	var p_operation_id string
	var a_languagecode_i string
	var a_username_i string
	var a_channelcode_i string
	var a_mcalist_i *godror.Object
	var a_validfrom_i string
	var a_validto_i string
	var a_statuscode_list_i string
	var a_type_list_o driver.Rows

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	typ, err := godror.GetObjectType(ctx, conn, "test_cwo_tbl_t")
	if err != nil {
		t.Fatal(err)
	}
	if a_mcalist_i, err = typ.NewObject(); err != nil {
		t.Fatal(err)
	}
	if typ, err = godror.GetObjectType(ctx, conn, "test_cwo_rec_t"); err != nil {
		t.Fatal("GetObjectType(test_cwo_rec_t):", err)
	}
	elt, err := typ.NewObject()
	if err != nil {
		t.Fatalf("NewObject(%s): %+v", typ, err)
	}
	if err = elt.Set("NUMBERPART1", "np1"); err != nil {
		t.Fatal("set NUMBERPART1:", err)
	}
	if err = a_mcalist_i.Collection().Append(elt); err != nil {
		t.Fatal("append to collection:", err)
	}

	const qry = `BEGIN test_cwo_getSum(:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9); END;`
	if _, err := conn.ExecContext(ctx, qry,
		sql.Named("v1", sql.Out{Dest: &p_operation_id, In: true}),
		sql.Named("v2", &a_languagecode_i),
		sql.Named("v3", &a_username_i),
		sql.Named("v4", &a_channelcode_i),
		sql.Named("v5", &a_mcalist_i),
		sql.Named("v6", &a_validfrom_i),
		sql.Named("v7", &a_validto_i),
		sql.Named("v8", &a_statuscode_list_i),
		sql.Named("v9", sql.Out{Dest: &a_type_list_o}),
	); err != nil {
		t.Fatal(err)
	}
	defer a_type_list_o.Close()
	t.Logf("%[1]p %#[1]v", a_type_list_o)

	rows, err := godror.WrapRows(ctx, conn, a_type_list_o)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var n int
		if err = rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		i++
		t.Logf("%d. %d", i, n)
	}
	// Test the Finalizers.
	runtime.GC()
}

func BenchmarkObjArray(b *testing.B) {
	cleanup := func() { testDb.Exec("DROP FUNCTION test_objarr"); testDb.Exec("DROP TYPE test_vc2000_arr") }
	cleanup()
	qry := "CREATE OR REPLACE TYPE test_vc2000_arr AS TABLE OF VARCHAR2(2000)"
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer cleanup()
	qry = `CREATE OR REPLACE FUNCTION test_objarr(p_arr IN test_vc2000_arr) RETURN PLS_INTEGER IS BEGIN RETURN p_arr.COUNT; END;`
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	ctx, cancel := context.WithCancel(testContext("BenchmarObjArray"))
	defer cancel()

	b.Run("object", func(b *testing.B) {
		b.StopTimer()
		const qry = `BEGIN :1 := test_objarr(:2); END;`
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		defer stmt.Close()
		typ, err := godror.GetObjectType(ctx, testDb, "TEST_VC2000_ARR")
		if err != nil {
			b.Fatal(err)
		}
		obj, err := typ.NewObject()
		if err != nil {
			b.Fatal(err)
		}
		defer obj.Close()
		coll := obj.Collection()

		var rc int
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			length, err := coll.Len()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if b.N < 1024 {
				for length < b.N {
					if err = coll.Append(fmt.Sprintf("--test--%010d--", i)); err != nil {
						b.Fatal(err)
					}
					length++
				}
			}
			if _, err := stmt.ExecContext(ctx, sql.Out{Dest: &rc}, obj); err != nil {
				b.Fatal(err)
			}
			if rc != length {
				b.Error("got", rc, "wanted", length)
			}
		}
	})

	b.Run("plsarr", func(b *testing.B) {
		b.StopTimer()
		const qry = `DECLARE
  TYPE vc2000_tab_typ IS TABLE OF VARCHAR2(2000) INDEX BY PLS_INTEGER;
  v_tbl vc2000_tab_typ := :1;
  v_idx PLS_INTEGER;
  v_arr test_vc2000_arr := test_vc2000_arr();
BEGIN
  -- copy the PL/SQL associative array to the nested table:
  v_idx := v_tbl.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_arr.EXTEND;
    v_arr(v_arr.LAST) := v_tbl(v_idx);
    v_idx := v_tbl.NEXT(v_idx);
  END LOOP;
  -- call the procedure:
  :2 := test_objarr(p_arr=>v_arr);
END;`
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		defer stmt.Close()
		b.StartTimer()

		var rc int
		var array []string
		for i := 0; i < b.N; i++ {
			if b.N < 1024 {
				for len(array) < b.N {
					array = append(array, fmt.Sprintf("--test--%010d--", i))
				}
			}
			if _, err := stmt.ExecContext(ctx, godror.PlSQLArrays, array, sql.Out{Dest: &rc}); err != nil {
				b.Fatal(err)
			}
			if rc != len(array) {
				b.Error(rc)
			}
		}
	})
}

// See https://github.com/godror/godror/issues/179
func TestObjectTypeClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ObjectTypeClose"), 30*time.Second)
	defer cancel()
	const typeName = "test_typeclose_t"
	const del = `DROP TYPE ` + typeName + ` CASCADE`
	testDb.ExecContext(ctx, del)
	// createType
	const ddl = `create or replace type ` + typeName + ` force as object (
     id NUMBER(10),  
	 balance NUMBER(18));`
	_, err := testDb.ExecContext(ctx, ddl)
	if err != nil {
		t.Fatalf("%s: %+v", ddl, err)
	}
	defer testDb.ExecContext(context.Background(), del)

	getObjectType := func(ctx context.Context, db *sql.DB) error {
		cx, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer cx.Close()

		objType, err := godror.GetObjectType(ctx, cx, typeName)
		if err != nil {
			return err
		}
		defer objType.Close()

		return nil
	}

	const maxConn = maxSessions * 2
	for j := 0; j < 5; j++ {
		t.Logf("Run %d group\n", j)
		var start sync.WaitGroup
		g, ctx := errgroup.WithContext(ctx)
		start.Add(1)
		for i := 0; i < maxConn/2; i++ {
			g.Go(func() error {
				start.Wait()
				return getObjectType(ctx, testDb)
			})
		}
		start.Done()
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

// See https://github.com/godror/godror/issues/180
func TestSubObjectTypeClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("SubObjectTypeClose"), 30*time.Second)
	defer cancel()
	const typeName = "test_subtypeclose"
	dels := []string{
		`DROP TYPE ` + typeName + `_ot CASCADE`,
		`DROP TYPE ` + typeName + `_lt CASCADE`,
	}
	for _, del := range dels {
		testDb.ExecContext(ctx, del)
	}
	// createType
	for _, ddl := range []string{
		`CREATE OR REPLACE TYPE ` + typeName + `_lt FORCE AS VARRAY(30) OF VARCHAR2(30);`,
		`CREATE OR REPLACE TYPE ` + typeName + `_ot FORCE AS OBJECT (
     id NUMBER(10),  
	 list ` + typeName + `_lt);`,
	} {
		_, err := testDb.ExecContext(ctx, ddl)
		if err != nil {
			t.Fatalf("%s: %+v", ddl, err)
		}
	}
	defer func() {
		for _, del := range dels {
			_, _ = testDb.ExecContext(context.Background(), del)
		}
	}()

	getObjectType := func(ctx context.Context, db *sql.DB) error {
		cx, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer cx.Close()

		objType, err := godror.GetObjectType(ctx, cx, typeName+"_ot")
		if err != nil {
			return err
		}
		defer objType.Close()

		return nil
	}

	const maxConn = maxSessions * 2
	for j := 0; j < 5; j++ {
		t.Logf("Run %d group\n", j)
		var start sync.WaitGroup
		g, ctx := errgroup.WithContext(ctx)
		start.Add(1)
		for i := 0; i < maxConn/2; i++ {
			g.Go(func() error {
				start.Wait()
				return getObjectType(ctx, testDb)
			})
		}
		start.Done()
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}
