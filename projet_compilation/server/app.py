import gradio as gr
import subprocess

def greet(code):
    compile = 'javaCompiler'
    output_data_err = subprocess.run([compile],input=code.encode(),stderr=subprocess.PIPE).stderr.decode()
    output_data_suc = subprocess.run([compile],input=code.encode(),stdout=subprocess.PIPE).stdout.decode()
    return output_data_suc,output_data_err

with gr.Blocks() as demo:
    code = gr.Code(interactive=True,language='javascript',label="Code")
    greet_btn = gr.Button("Compile")

    with gr.Row():
        output_err = gr.Textbox(label='errors')
        output_suc = gr.Textbox(label='success')

    greet_btn.click(fn=greet, inputs=code, outputs=[output_suc,output_err])



demo.launch()   