# docs

Approach, architecture and POC status for the messaging + voice gateway.

```bash
pip install python-docx cairosvg
python docs/diagrams.py     # docs/out/*.svg (+ .png copies)
python docs/build_docx.py   # regenerates diagrams, then docs/out/voitta-gateway.docx
```

- `svgkit.py`: small dependency-free SVG helper (boxes, arrows, sequence diagrams)
- `diagrams.py`: architecture with status badges, message / call / voitta-ask flows,
  POC and target deployment, roadmap
- `build_docx.py`: the document text; edit here, not in the .docx
