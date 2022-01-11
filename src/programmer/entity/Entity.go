package entity

type M map[string]interface{}

type Data_pegawai struct {
	Id     string `json:"Id"`
	Nip    string `json:"Nip"`
	Nama   string `json:"Nama"`
	Status string `json:"Status"`
}

type Data_pegawai_Collection struct {
	Data_pegawais []Data_pegawai
}
