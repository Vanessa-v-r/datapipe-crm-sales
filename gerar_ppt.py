from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch  # Importando inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
import matplotlib.pyplot as plt
import os

# Define colors in ReportLab format
verde = colors.Color(141/255, 198/255, 63/255)
marrom = colors.Color(211/255, 79/255, 29/255)
marrom_escuro = colors.Color(147/255, 67/255, 5/255)
off_white = colors.Color(244/255, 244/255, 244/255)

# Define styles
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='TitleStyle', fontSize=24, leading=28, alignment=1, textColor=marrom_escuro))
styles.add(ParagraphStyle(name='SubtitleStyle', fontSize=18, leading=22, alignment=1, textColor=marrom))
styles.add(ParagraphStyle(name='BodyStyle', fontSize=12, leading=16, alignment=4, textColor=colors.black))
styles.add(ParagraphStyle(name='BulletStyle', fontSize=12, leading=16, alignment=4, leftIndent=20, bulletIndent=10, bulletFontSize=12, bulletColor=marrom_escuro))

# Create chart using matplotlib
def create_chart():
    current_clients = 210
    potential_clients = 12866

    clients = ['Clientes Atuais', 'Potencial de Clientes']
    numbers = [current_clients, potential_clients]

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(clients, numbers, color=['#D34F1D', '#8DC63F'])
    ax.set_ylabel('Número de Clientes')
    ax.set_title('Comparação de Clientes Atuais vs Potenciais')
    ax.bar_label(bars, padding=3)
    plt.tight_layout()
    chart_filename = 'temp_chart.png'
    plt.savefig(chart_filename, dpi=300)
    plt.close()
    return chart_filename

# Function to create the document
def create_pdf(filename):
    doc = SimpleDocTemplate(filename, pagesize=A4)
    Story = []

    # Title Slide
    Story.append(Spacer(1, 2 * cm))
    title = Paragraph("Projeto de Business Intelligence", styles['TitleStyle'])
    subtitle = Paragraph("Organização da Base de Negócios e Potencial de Crescimento", styles['SubtitleStyle'])
    Story.append(title)
    Story.append(Spacer(1, 0.5 * cm))
    Story.append(subtitle)
    Story.append(Spacer(1, 1 * cm))

    # Slide 2: Objetivos do Projeto
    heading = Paragraph("Objetivos do Projeto", styles['TitleStyle'])
    objectives = [
        "Criação e Automação de Dashboards de Monitoramento",
        "Organização de Leads Comerciais para impulsionar a operação de Sales",
        "Piloto para criação e integração com um sistema CRM robusto",
    ]
    bullet_points = [Paragraph(f'• {obj}', styles['BulletStyle']) for obj in objectives]
    Story.extend([heading] + bullet_points)
    Story.append(Spacer(1, 1 * cm))

    # Slide 3: Situação Atual
    heading = Paragraph("Situação Atual", styles['TitleStyle'])
    current_state = (
        "Atualmente, o processo é realizado inteiramente no Notion, sem validação de unicidade de registros "
        "de empresas, contatos ou relação entre ambos. Cada linha é uma entidade isolada, tornando análises "
        "ineficientes ou impossíveis devido à ausência de relacionamento entre as páginas do Notion."
    )
    body = Paragraph(current_state, styles['BodyStyle'])
    Story.extend([heading, body])
    Story.append(Spacer(1, 1 * cm))

    # Slide 4: Solução Proposta
    heading = Paragraph("Solução Proposta", styles['TitleStyle'])
    solution = (
        "Implementação de um pipeline ETL robusto que extrai, transforma e carrega os dados do Notion "
        "para uma infraestrutura mais sólida, permitindo análises avançadas e suporte à tomada de decisão."
    )
    body = Paragraph(solution, styles['BodyStyle'])
    Story.extend([heading, body])
    Story.append(Spacer(1, 1 * cm))

    # Slide 5: Potencial de Crescimento
    heading = Paragraph("Potencial de Crescimento", styles['TitleStyle'])
    Story.append(heading)
    chart_filename = create_chart()
    img = Image(chart_filename, width=6 * inch, height=4 * inch)
    Story.append(img)
    os.remove(chart_filename)
    Story.append(Spacer(1, 0.5 * cm))

    insights = [
        "Atualmente atendemos apenas 210 clientes.",
        "Existe um potencial de mercado de mais de 12.800 clientes.",
        "Com a nova estrutura, podemos aumentar significativamente nossa participação de mercado.",
    ]
    bullet_points = [Paragraph(f'• {insight}', styles['BulletStyle']) for insight in insights]
    Story.extend(bullet_points)
    Story.append(Spacer(1, 1 * cm))

    # Slide 6: Benefícios para a Equipe de Sales
    heading = Paragraph("Benefícios para a Equipe de Sales", styles['TitleStyle'])
    benefits = [
        "Visão unificada do Cliente, permitindo abordagens mais assertivas.",
        "Redução do tempo gasto em tarefas manuais e repetitivas.",
        "Aumento da eficiência na conversão de Leads em Clientes.",
        "Identificação de oportunidades de upsell e cross-sell.",
    ]
    bullet_points = [Paragraph(f'• {benefit}', styles['BulletStyle']) for benefit in benefits]
    Story.extend([heading] + bullet_points)
    Story.append(Spacer(1, 1 * cm))

    # Slide 7: Impacto no Negócio
    heading = Paragraph("Impacto no Negócio", styles['TitleStyle'])
    impact = (
        "Com a implementação do novo sistema, espera-se um aumento significativo na eficiência "
        "operacional e nas vendas, contribuindo para a retomada do crescimento da receita."
    )
    body = Paragraph(impact, styles['BodyStyle'])
    Story.extend([heading, body])
    Story.append(Spacer(1, 1 * cm))

    # Slide 8: Próximos Passos
    heading = Paragraph("Próximos Passos", styles['TitleStyle'])
    next_steps = [
        "Concluir testes e validações do pipeline ETL (previsão de mais uma semana).",
        "Implementar o sistema CRM integrado.",
        "Treinar a equipe de Sales no uso das novas ferramentas.",
        "Monitorar métricas-chave para ajustes contínuos.",
    ]
    bullet_points = [Paragraph(f'• {step}', styles['BulletStyle']) for step in next_steps]
    Story.extend([heading] + bullet_points)
    Story.append(Spacer(1, 1 * cm))

    # Slide 9: Considerações Técnicas
    heading = Paragraph("Considerações Técnicas", styles['TitleStyle'])
    technical_points = [
        "O projeto envolve diversas áreas de expertise técnica simultaneamente.",
        "O tempo investido no mapeamento de pré-requisitos e desenvolvimento é essencial para a qualidade do deliverable.",
        "A complexidade do projeto é alta para um profissional em nível Inter/Junior, justificando o tempo necessário.",
    ]
    bullet_points = [Paragraph(point, styles['BulletStyle']) for point in technical_points]
    Story.extend([heading] + bullet_points)
    Story.append(Spacer(1, 1 * cm))

    # Slide 10: Agradecimentos
    heading = Paragraph("Agradecimentos", styles['TitleStyle'])
    thanks = (
        "Obrigado pela atenção! Estou à disposição para responder perguntas e discutir "
        "como esse projeto impulsionará nossos resultados em Sales e Produto."
    )
    body = Paragraph(thanks, styles['BodyStyle'])
    Story.extend([heading, body])

    # Build the PDF
    doc.build(Story)

# Generate the PDF presentation
create_pdf("apresentacao_projeto_bi.pdf")
