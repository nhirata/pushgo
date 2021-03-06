// Automatically generated by MockGen. DO NOT EDIT!
// Source: src/github.com/mozilla-services/pushgo/simplepush/config.go

package simplepush

import (
	gomock "github.com/rafrombrc/gomock/gomock"
)

// Mock of HasConfigStruct interface
type MockHasConfigStruct struct {
	ctrl     *gomock.Controller
	recorder *_MockHasConfigStructRecorder
}

// Recorder for MockHasConfigStruct (not exported)
type _MockHasConfigStructRecorder struct {
	mock *MockHasConfigStruct
}

func NewMockHasConfigStruct(ctrl *gomock.Controller) *MockHasConfigStruct {
	mock := &MockHasConfigStruct{ctrl: ctrl}
	mock.recorder = &_MockHasConfigStructRecorder{mock}
	return mock
}

func (_m *MockHasConfigStruct) EXPECT() *_MockHasConfigStructRecorder {
	return _m.recorder
}

func (_m *MockHasConfigStruct) ConfigStruct() interface{} {
	ret := _m.ctrl.Call(_m, "ConfigStruct")
	ret0, _ := ret[0].(interface{})
	return ret0
}

func (_mr *_MockHasConfigStructRecorder) ConfigStruct() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ConfigStruct")
}

func (_m *MockHasConfigStruct) Init(app *Application, config interface{}) error {
	ret := _m.ctrl.Call(_m, "Init", app, config)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockHasConfigStructRecorder) Init(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Init", arg0, arg1)
}
