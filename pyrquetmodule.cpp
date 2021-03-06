/*
TODO
deal with encoding issues
*/
#include "Python.h"
#include <sys/mman.h>
#include <arrow/io/memory.h>
#define private public
#include <parquet/api/reader.h>
#include <parquet/column_scanner.h>
#include <parquet/types.h>

typedef struct {
    PyObject_HEAD
    std::unique_ptr<parquet::ParquetFileReader> file;
    std::vector<std::tuple<std::string, std::shared_ptr<parquet::Scanner>>> scanners;
    int row_group;
} PyParquetGenState;

static PyObject *
pyrquetgen_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    int fd;

    if (!PyArg_ParseTuple(args, "i", &fd))
        return NULL;

    // Create a new PyParquetGenState and initialize its state
    PyParquetGenState *ppstate = (PyParquetGenState *)type->tp_alloc(type, 0);
    if (!ppstate)
        return NULL;

    struct stat file_stat;
    fstat(fd, &file_stat);
    size_t size_ = file_stat.st_size;
    void* mmap_ptr = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_ptr == MAP_FAILED) {
        PyErr_SetString(PyExc_RuntimeError, "mmap failed");
        return NULL;
    }

    std::unique_ptr<arrow::io::BufferReader> ptr(new arrow::io::BufferReader(reinterpret_cast<uint8_t*>(mmap_ptr), size_));
    ppstate->file = parquet::ParquetFileReader::Open(std::move(ptr));
    ppstate->row_group = 0;
    return (PyObject*)ppstate;
}

static void
pyrquetgen_dealloc(PyParquetGenState *ppstate)
{
    Py_TYPE(ppstate)->tp_free(ppstate);
}

template <typename DType>
class MyScanner : public parquet::TypedScanner<DType> {
    public:
      typedef typename DType::c_type T;

      explicit MyScanner(std::shared_ptr<parquet::ColumnReader> reader,
                            int64_t batch_size = 128,
                            ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
          : parquet::TypedScanner<DType>(reader, batch_size, pool) {}

      void PrintNext(std::ostream& out, int width) override {
        T val;
        bool is_null = false;
        char buffer[1 << 15];

        if (!this->NextValue(&val, &is_null)) {
          throw parquet::ParquetException("No more values buffered");
        }

        if (is_null)
          return;
        this->FormatValue(&val, buffer, sizeof(buffer), width);
        out << buffer;
      }
};

std::shared_ptr<parquet::Scanner>
my_scanner_make(std::shared_ptr<parquet::ColumnReader> col_reader) {
  switch (col_reader->type()) {
    case parquet::Type::BOOLEAN:
      return std::make_shared<MyScanner<parquet::BooleanType>>(col_reader);
    case parquet::Type::INT32:
      return std::make_shared<MyScanner<parquet::Int32Type>>(col_reader);
    case parquet::Type::INT64:
      return std::make_shared<MyScanner<parquet::Int64Type>>(col_reader);
    case parquet::Type::INT96:
      return std::make_shared<MyScanner<parquet::Int96Type>>(col_reader);
    case parquet::Type::FLOAT:
      return std::make_shared<MyScanner<parquet::FloatType>>(col_reader);
    case parquet::Type::DOUBLE:
      return std::make_shared<MyScanner<parquet::DoubleType>>(col_reader);
    case parquet::Type::BYTE_ARRAY:
      return std::make_shared<MyScanner<parquet::ByteArrayType>>(col_reader);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<MyScanner<parquet::FLBAType>>(col_reader);
    default:
      parquet::ParquetException::NYI("type reader not implemented");
  // Unreachable code, but suppress compiler warning
  return std::shared_ptr<parquet::Scanner>(nullptr);
  }
}

static PyObject *
pyrquetgen_next(PyParquetGenState *ppstate)
{
    while (ppstate->row_group < ppstate->file->metadata()->num_row_groups()) {
        if (ppstate->scanners.size() == 0) {
            auto group_reader = ppstate->file->RowGroup(ppstate->row_group);
            for (int i = 0; i < ppstate->file->metadata()->num_columns(); i++) {
              std::shared_ptr<parquet::ColumnReader> col_reader = group_reader->Column(i);
              ppstate->scanners.push_back(std::make_tuple(ppstate->file->metadata()->schema()->Column(i)->path()->ToDotString(),
                                                          my_scanner_make(col_reader)));
            }
        }

        PyObject* row = PyDict_New();
        bool hasRow = false;
        for (auto t : ppstate->scanners) {
            auto s = std::get<1>(t);
            auto c = std::get<0>(t).c_str();
            if (s->HasNext()) {
                hasRow = true;
                std::ostringstream stream;
                s->PrintNext(stream, 0);
                if (stream.rdbuf()->in_avail() == 0)
                    continue;
                PyDict_SetItem(row, PyUnicode_FromString(c), PyUnicode_Decode(stream.str().c_str(), stream.str().size(), "utf-8", "replace"));
            }
        }

        if (hasRow) {
            return row;
        } else {
            ppstate->scanners.clear();
            ppstate->row_group++;
            continue;
        }
    }

    /* Raising of standard StopIteration exception with empty value. */
    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
}

PyTypeObject PyParquetGen_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "open",                         /* tp_name */
    sizeof(PyParquetGenState),      /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor)pyrquetgen_dealloc, /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_reserved */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    0,                              /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    0,                              /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    PyObject_SelfIter,              /* tp_iter */
    (iternextfunc)pyrquetgen_next,  /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
    PyType_GenericAlloc,            /* tp_alloc */
    pyrquetgen_new,                 /* tp_new */
};

static struct
PyModuleDef pyrquetmodule = {
   PyModuleDef_HEAD_INIT,
   "pyrquet",                  /* m_name */
   NULL,                      /* m_doc */
   -1,                      /* m_size */
};

PyMODINIT_FUNC
PyInit_pyrquet(void)
{
    PyObject *module = PyModule_Create(&pyrquetmodule);
    if (!module)
        return NULL;

    if (PyType_Ready(&PyParquetGen_Type) < 0)
        return NULL;
    Py_INCREF((PyObject *)&PyParquetGen_Type);
    PyModule_AddObject(module, "open", (PyObject *)&PyParquetGen_Type);

    return module;
}
