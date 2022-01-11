package entity

type Barang struct {
	IdBarang   string `json:"idbarang"`
	NamaBarang string `json:"namabarang"`
	Deskripsi  string `json:"deskripsi"`
	Lokasi     string `json:"lokasi"`
	FotoBarang string `json:"fotobarang"`
}
