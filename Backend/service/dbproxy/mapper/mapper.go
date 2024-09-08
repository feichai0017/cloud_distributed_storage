package mapper

import (
	"cloud_distributed_storage/Backend/service/dbproxy/orm"
	"fmt"
	"reflect"
)

var funcs = map[string]interface{}{
	"/file/OnFileUploadFinished": orm.OnFileUploadFinished,
	"/file/GetFileMeta":          orm.GetFileMeta,
	"/file/GetFileMetaList":      orm.GetFileMetaList,
	"/file/UpdateFileLocation":   orm.UpdateFileLocation,

	"/user/UserSignup":  orm.UserSignup,
	"/user/UserLogin":   orm.UserLogin,
	"/user/UserExist":   orm.UserExist,
	"/user/UpdateToken": orm.UpdateToken,
	"/user/GetUserInfo": orm.GetUserInfo,

	"/ufile/OnUserFileUploadFinished": orm.OnUserFileUploadFinished,
	"/ufile/QueryUserFileMetas":       orm.QueryUserFileMetas,
	"/ufile/QueryUserFileMeta":        orm.QueryUserFileMeta,
	"/ufile/UpdateUserFileName":       orm.UpdateFileName,
	"/ufile/DeleteUserFile":           orm.DeleteUserFile,
}

func FunCall(name string, params ...interface{}) (result []reflect.Value, err error) {
	f, ok := funcs[name]
	if !ok {
		err = fmt.Errorf("func %s not found", name)
		return
	}
	// use reflect to call the function
	fv := reflect.ValueOf(f)
	if len(params) != fv.Type().NumIn() {
		err = fmt.Errorf("func %s need %d params", name, fv.Type().NumIn())
		return
	}
	// construct a slice of reflect.Value
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = fv.Call(in)
	return
}