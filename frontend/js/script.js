const body = document.querySelector('#table tbody');

fetch('https://api.allorigins.win/raw?url=https://xcmagg.lvido.tech/data.jsonl')
	.then(response => response.text())
	.then(text => {
		const lines = text.trim().split('\n').filter(Boolean);
		const jsonArray = lines.map(line => JSON.parse(line));
		jsonArray.forEach(item => {
			const row = document.createElement('tr');
			row.innerHTML = `
       <td data-label="Título">${item.title}</td>
       <td data-label="Data">${item.date_range.date_raw}</td>
       <td data-label="Local">${item.location.location_raw}</td>
       <td><a href="${item.url}" target="_blank">Acessar</a></td>
       `;
			body.appendChild(row);
		});
	})
	.catch(error => console.error('Erro:', error));

