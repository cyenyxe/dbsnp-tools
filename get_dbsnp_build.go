package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	/*"net/http"*/
	"github.com/jlaffaye/ftp"
	"os"
	"regexp"
	"sort"
	"strconv"
)

type Species struct {
	DatabaseName          string
	SubmittedTaxId        string
	SpeciesTaxId          string
	DbsnpBuildPublic      string
	Category              string
	ReferenceAssemblyName string
	ReferenceAssembly     string
	GenbankAssembly       string
	UnclusteredSsCount    string
	SsCount               string
	RsCount               string
	SsCountDate           string
	RsCountDate           string
	InPublicDate          string
	PriorityEbi           string
	Build151Status        string
}

func main() {
	csvInputFile, err := os.Open("EBI_nonhuman_VR_71_VR_69.csv")
	if err != nil {
		log.Fatal(err)
	}

	csvOutputFile, err := os.Create("EBI_nonhuman_VR_71_VR_69.output.csv")
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvInputFile))
	writer := csv.NewWriter(bufio.NewWriter(csvOutputFile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		species := Species{
			DatabaseName:          line[0],
			SubmittedTaxId:        line[1],
			SpeciesTaxId:          line[2],
			DbsnpBuildPublic:      line[3],
			Category:              line[4],
			ReferenceAssemblyName: line[5],
			ReferenceAssembly:     line[6],
			GenbankAssembly:       line[7],
			UnclusteredSsCount:    line[8],
			SsCount:               line[9],
			RsCount:               line[10],
			SsCountDate:           line[11],
			RsCountDate:           line[12],
			InPublicDate:          line[13],
			PriorityEbi:           line[14],
			Build151Status:        line[15],
		}

		// Search dbSNP FTP for folder ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/<DatabaseName>/database/organism_data/
		client, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21")
		if err != nil {
			log.Fatal(err)
		}

		if err := client.Login("anonymous", "anonymous"); err != nil {
			log.Fatal(err)
		}

		fmt.Println(species.DatabaseName)
		entries, err := client.List("/snp/organisms/" + species.DatabaseName + "/database/organism_data/")
		if err != nil {
			continue
		}

		// Get build numbers
		builds := make([]int, 0)
		r := regexp.MustCompile("^b(?P<buildnum>\\d{3})_SNPContigLoc(_\\d+)*\\.bcp\\.gz$")
		for _, entry := range entries {
			name := entry.Name
			if match := r.FindStringSubmatch(name); match != nil {
				i, err := strconv.Atoi(match[1])
				if err != nil {
					log.Printf("%v is not a valid build number", match[1])
				} else {
					builds = append(builds, i)
				}
			}
		}

		// Get greatest build number
		sort.Ints(builds)
		line[3] = strconv.Itoa(builds[len(builds)-1])

		// Print to screen and write to CSV
		fmt.Println(builds[len(builds)-1])
		writer.Write(line)
		writer.Flush()
	}
}
