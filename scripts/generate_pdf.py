"""One-time script to convert README.md to a clean PDF."""
import markdown
import fpdf
import fpdf.html
from fpdf import FPDF


def sanitize(text):
    """Replace Unicode chars that latin-1 can't encode."""
    replacements = {
        "\u2014": "--", "\u2013": "-", "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"', "\u2026": "...", "\u00d7": "x",
        "\u2192": "->", "\u00a0": " ", "\u2248": "~", "\u2265": ">=",
        "\u2264": "<=", "\u00b0": "deg",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.encode("latin-1", errors="replace").decode("latin-1")


class PDF(FPDF):
    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Mirror Market  |  Page {self.page_no()}/{{nb}}", align="C")
        self.set_text_color(0, 0, 0)


def make_pdf(md_path="README.md", out_path="Mirror_Market_Overview.pdf"):
    with open(md_path) as f:
        md_text = sanitize(f.read())

    import re

    # Convert markdown to HTML
    html = markdown.markdown(md_text, extensions=["tables", "fenced_code"])

    # fpdf2 doesn't support nested tags inside <td>/<th> — strip them
    def strip_tags_in_cells(match):
        content = match.group(1)
        # Remove any HTML tags inside the cell
        content = re.sub(r"<[^>]+>", "", content)
        return f"<td>{content}</td>"

    def strip_tags_in_th(match):
        content = match.group(1)
        content = re.sub(r"<[^>]+>", "", content)
        return f"<th>{content}</th>"

    html = re.sub(r"<td>(.*?)</td>", strip_tags_in_cells, html, flags=re.DOTALL)
    html = re.sub(r"<th>(.*?)</th>", strip_tags_in_th, html, flags=re.DOTALL)

    # Set reasonable table width
    html = html.replace("<table>", '<table width="100%">')

    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    pdf.set_font("Helvetica", size=9)

    # Use fpdf2's write_html for proper rendering
    pdf.write_html(html, tag_styles={
        "h1": fpdf.html.FontFace(color=(0, 51, 102), size_pt=22),
        "h2": fpdf.html.FontFace(color=(0, 71, 122), size_pt=15),
        "h3": fpdf.html.FontFace(color=(51, 51, 51), size_pt=12),
    })

    pdf.output(out_path)
    print(f"PDF saved to: {out_path}")


if __name__ == "__main__":
    make_pdf()
