#include "Python.h"

#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include "hadoop/Hce.hh"
#include "hadoop/TemplateFactoryHce.hh"
#include "Log.hh"
#include "TaskContextImpl.hh"

static void set_PYTHONHOME(){
  if (NULL==getenv("PYTHONHOME")){
    const char * pwd = getenv("PWD");
    if (NULL!=pwd){
      string ph = pwd;
      ph = ph + "/python2.7";
      struct stat st;
      if (0==stat(ph.c_str(),&st)){
        if (S_ISDIR(st.st_mode)){
          setenv("PYTHONHOME",(char*)ph.c_str(),1);
          return;
        }
      }
    }
    const char * hadoophome = getenv("HADOOPHOME");
    if (NULL!=hadoophome){
      string ph = hadoophome;
      ph += "/libhce/pyhce/python2.7";
      struct stat st;
      if (0==stat(ph.c_str(),&st)){
        if (S_ISDIR(st.st_mode)){
          setenv("PYTHONHOME",(char*)ph.c_str(),1);
          return;
        }
      }
    }
    HCE_WRITE_LOG(HCE_LOG_WARNING, "Can't find proper PYTHONHOME for pyhce!!");
  }
}

typedef struct {
  PyObject_HEAD
  HCE::ReduceInput * preduce_input;
} ReduceValues;

static PyObject *
ReduceValues_self(PyObject * obj)
{
  ReduceValues * rv = (ReduceValues*)obj;
  if (rv->preduce_input == NULL){
      PyErr_SetString(PyExc_ValueError, "Error closed or uninitialized ReduceInput");
      return NULL;
  }
  Py_INCREF(rv);
  return (PyObject *)rv;
}

static PyObject *
reducevalues_iternext(PyObject *obj){
  ReduceValues * rvs = (ReduceValues *)obj;
  if (rvs->preduce_input->nextValue()){
    int64_t valuelen;
    const char * value = (const char *)(rvs->preduce_input->value(valuelen));
    return PyString_FromStringAndSize(value, valuelen);
  }else{
    rvs->preduce_input = NULL;
    return NULL;
  }
}

static void ReduceValues_dealloc(ReduceValues* self) {
  self->ob_type->tp_free((PyObject*) self);
}

static PyObject * ReduceValues_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
  ReduceValues *self;
  self = (ReduceValues *) type->tp_alloc(type, 0);
  return (PyObject *) self;
}

static PyTypeObject ReduceValuesType = {
  PyObject_HEAD_INIT(NULL)0, /*ob_size*/
  "hceutil.ReduceValues", /*tp_name*/
  sizeof(ReduceValues), /*tp_basicsize*/
  0, /*tp_itemsize*/
  (destructor)ReduceValues_dealloc, /*tp_dealloc*/
  0, /*tp_print*/
  0, /*tp_getattr*/
  0, /*tp_setattr*/
  0, /*tp_compare*/
  0, /*tp_repr*/
  0, /*tp_as_number*/
  0, /*tp_as_sequence*/
  0, /*tp_as_mapping*/
  0, /*tp_hash */
  0, /*tp_call*/
  0, /*tp_str*/
  0, /*tp_getattro*/
  0, /*tp_setattro*/
  0, /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
  "ReduceValues objects", /* tp_doc */
  0, /* tp_traverse */
  0, /* tp_clear */
  0, /* tp_richcompare */
  0, /* tp_weaklistoffset */
  ReduceValues_self, /* tp_iter */
  reducevalues_iternext, /* tp_iternext */
  0, /* tp_methods */
  0, /* tp_members */
  0, /* tp_getset */
  0, /* tp_base */
  0, /* tp_dict */
  0, /* tp_descr_get */
  0, /* tp_descr_set */
  0, /* tp_dictoffset */
  0, /* tp_init */
  0, /* tp_alloc */
  ReduceValues_new, /* tp_new */
};


static HCE::TaskContextImpl * g_context = NULL;
static HCE::Partitioner * g_partitioner = NULL;
const char * g_modulefile = "pyhce.py";

static PyObject* emit(PyObject *self, PyObject *args) {
  const char * key;
  int keylen;
  const char * value;
  int valuelen;
  if (!PyArg_ParseTuple(args, "z#z#", &key, &keylen, &value, &valuelen))
    return NULL;
  g_context->emit(key, keylen, value, valuelen);
  Py_RETURN_NONE;
}

static PyMethodDef UtilMethods[] = {
  { "emit", emit, METH_VARARGS, "Emit key and value" },
  { NULL, NULL, 0, NULL }
};


class PyPartitioner: public HCE::Partitioner{
protected:
  PyObject * _partition_func;
public:
  PyPartitioner(PyObject * func_ptr):_partition_func(func_ptr){
  }
  int32_t partition(void* key, int64_t &keyLength, int numOfReduces){
    PyObject * args = PyTuple_New(2);
    PyTuple_SetItem(args,0,PyString_FromStringAndSize((const char *)key, keyLength));
    PyTuple_SetItem(args,1,PyInt_FromLong(numOfReduces));
    PyObject * ret = PyObject_CallObject(_partition_func, args);
    if (PyErr_Occurred()){
      PyErr_Print();
      return -1;
    }
    Py_DECREF(args);
    long retl = PyInt_AsLong(ret);
    if (PyErr_Occurred()){
      PyErr_Print();
      Py_XDECREF(ret);
      return -1;
    }
    Py_XDECREF(ret);
    return retl;
  }
};

// Note: PyHceMapper must be created before PyPartitioner creation
//
class PyHceMapper: public HCE::Mapper{
protected:
  bool _py_init;
  PyObject * _main_module;
  PyObject * _mapper_func;
public:
  PyHceMapper():_py_init(false),_main_module(false),_mapper_func(false){
    Py_Initialize();
    FILE * fin = fopen(g_modulefile, "r");
    if (NULL==fin){
      HCE_WRITE_LOG(HCE_LOG_FATAL,"python script %s not found!", g_modulefile);
      return;
    }
    Py_InitModule("hceutil", UtilMethods);
    if (0!=PyRun_SimpleString("from hceutil import emit")){
      HCE_WRITE_LOG(HCE_LOG_FATAL,"error when import hceutil.emit");
      return;
    }
    if (0!=PyRun_SimpleFile(fin,g_modulefile)){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "error when loading %s", g_modulefile);
      return;
    }
    _main_module = PyImport_AddModule("__main__");
    _mapper_func = PyObject_GetAttrString(_main_module, "mapper");
    if ((NULL == _mapper_func) || (0 == PyCallable_Check(_mapper_func))){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "mapper not exists or not a callable in %s", g_modulefile);
      return;
    }
    if (PyObject_HasAttrString(_main_module, "mapper_setup")){
      PyObject * mapper_setup_func = PyObject_GetAttrString(_main_module, "mapper_setup");
      PyObject * ret = PyObject_CallObject(mapper_setup_func, NULL);
      if (PyErr_Occurred()){
        PyErr_Print();
        return;
      }
      if (ret == Py_False){
        Py_DECREF(ret);
        return;
      }
      Py_XDECREF(ret);
      Py_DECREF(mapper_setup_func);
    }
    if (PyObject_HasAttrString(_main_module, "partitioner")){
      PyObject * partitioner_func = PyObject_GetAttrString(_main_module, "partitioner");
      g_partitioner = new PyPartitioner(partitioner_func);
    }
    _py_init = true;
  }
  ~PyHceMapper(){
    cleanup();
  }
  int64_t setup() {
    if (_py_init){
      g_context = (HCE::TaskContextImpl*)getContext();
      return 0;
    }else{
      return 1;
    }
  }

  int64_t map(HCE::MapInput &input) {
    int64_t keyLen;
    int64_t valLen;
    const void* keyBuf = input.key(keyLen);
    const void* valBuf = input.value(valLen);
    PyObject * args = PyTuple_New(2);
    PyTuple_SetItem(args,0,PyString_FromStringAndSize((const char *)keyBuf, keyLen));
    PyTuple_SetItem(args,1,PyString_FromStringAndSize((const char *)valBuf, valLen));
    PyObject * ret = PyObject_CallObject(_mapper_func, args);
    if (PyErr_Occurred()){
      PyErr_Print();
      return 1;
    }
    Py_DECREF(args);
    if (ret == Py_False){
      Py_DECREF(ret);
      return 1;
    }
    Py_XDECREF(ret);
    return 0;
  }

  int64_t cleanup() {
    int result = 0;
    if (_py_init){
      if (PyObject_HasAttrString(_main_module, "mapper_cleanup")){
        PyObject * mapper_clean_func = PyObject_GetAttrString(_main_module, "mapper_cleanup");
        PyObject * ret = PyObject_CallObject(mapper_clean_func, NULL);
        if (PyErr_Occurred()){
          PyErr_Print();
          result = 1;
        }else{
          if (ret == Py_False){
            Py_DECREF(ret);
            result = 1;
          }else{
            Py_XDECREF(ret);
          }
        }
        Py_DECREF(mapper_clean_func);
      }
//      Py_XDECREF(_mapper_func);
//      Py_XDECREF(_main_module);
      Py_Finalize();
      _py_init = false;
    }
    return result;
  }
};


class PyHceReducer: public HCE::Reducer {
protected:
  bool _py_init;
  PyObject * _main_module;
  PyObject * _reducer_func;
public:
  int64_t setup() {
    g_context = (HCE::TaskContextImpl*)getContext();
    Py_Initialize();
    _py_init = true;
    if (PyType_Ready(&ReduceValuesType) != 0){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "PyType_Ready(&ReduceValuesType) failed");
      return 1;
    }
    PyObject* hceutil_module = Py_InitModule("hceutil", UtilMethods);
    Py_INCREF(&ReduceValuesType);
    PyModule_AddObject(hceutil_module,"ReduceValues",(PyObject*)&ReduceValuesType);
    if (0!=PyRun_SimpleString("from hceutil import emit")){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "error when import hceutil.emit");
      return 1;
    }
    FILE * fin = fopen(g_modulefile, "r");
    if (NULL==fin){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "python script %s not found!", g_modulefile);
      return 1;
    }
    if (0!=PyRun_SimpleFile(fin,g_modulefile)){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "error when loading %s", g_modulefile);
      fclose(fin);
      return 1;
    }
    fclose(fin);
    _main_module = PyImport_AddModule("__main__");
    _reducer_func = PyObject_GetAttrString(_main_module, "reducer");
    if ((NULL == _reducer_func) || (0 == PyCallable_Check(_reducer_func))){
      HCE_WRITE_LOG(HCE_LOG_FATAL, "reducer not exists or not a callable in %s\n", g_modulefile);
      return 1;
    }
    if (PyObject_HasAttrString(_main_module, "reducer_setup")){
      PyObject * reducer_setup_func = PyObject_GetAttrString(_main_module, "reducer_setup");
      PyObject * ret = PyObject_CallObject(reducer_setup_func, NULL);
      if (PyErr_Occurred()){
        PyErr_Print();
        return 1;
      }
      if (ret == Py_False){
        Py_DECREF(ret);
        return 1;
      }
      Py_XDECREF(ret);
      Py_DECREF(reducer_setup_func);
    }
    return 0;
  }
  int64_t reduce(HCE::ReduceInput &input) {
    int64_t keyLen;
    int64_t valLen;
    const void* keyBuf = input.key(keyLen);
    PyObject * args = PyTuple_New(2);
    PyObject * keyobj = PyString_FromStringAndSize((const char *)keyBuf,keyLen);
    PyTuple_SetItem(args,0,keyobj);
    PyObject * rvs = ReduceValues_new(&ReduceValuesType, NULL, NULL);
    ((ReduceValues*)rvs)->preduce_input = &input;
    PyTuple_SetItem(args,1,rvs);
    PyObject * ret = PyObject_CallObject(_reducer_func, args);
    if (PyErr_Occurred()){
      PyErr_Print();
      return 1;
    }
    Py_DECREF(args);
    if (ret == Py_False){
      Py_DECREF(ret);
      return 1;
    }
    Py_XDECREF(ret);
    return 0;
  }
  int64_t cleanup() {
    int result = 0;
    if (_py_init){
      if (PyObject_HasAttrString(_main_module, "reducer_cleanup")){
        PyObject * reducer_clean_func = PyObject_GetAttrString(_main_module, "reducer_cleanup");
        PyObject * ret = PyObject_CallObject(reducer_clean_func, NULL);
        if (PyErr_Occurred()){
          PyErr_Print();
          result = 1;
        }else{
          if (ret == Py_False){
            Py_DECREF(ret);
            result = 1;
          }else{
            Py_XDECREF(ret);
          }
        }
        Py_DECREF(reducer_clean_func);
      }
//      Py_XDECREF(_reducer_func);
//      Py_XDECREF(_main_module);
      Py_Finalize();
      _py_init = false;
    }
    return result;
  }
};

class PyHceFactory: public HCE::Factory {
public:
  HCE::Mapper* createMapper() const {
    return new PyHceMapper();
  }
  HCE::Reducer* createReducer() const {
    return new PyHceReducer();
  }
  HCE::Partitioner* createPartitioner() const {
    return g_partitioner;
  }
};

int main(int argc, char **argv) {
  if (argc>1){
    g_modulefile = argv[1];
  }
  set_PYTHONHOME();
  HCE_WRITE_LOG(HCE_LOG_NOTICE, "PYTHONHOME: %s", getenv("PYTHONHOME"));
  if (HCE::runTask(PyHceFactory()))
    return 0;
  else
    return 1;
}
